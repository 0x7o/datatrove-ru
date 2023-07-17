"""
'To deduplicate the data set, we discarded all but one of any three-sentence span
occurring more than once in the data set.'

from: https://jmlr.org/papers/volume21/20-074/20-074.pdf (C4)

# get hashes for each doc and write them down

"""
import heapq
import struct
from dataclasses import dataclass
from typing import Generator

from nltk.tokenize import sent_tokenize, word_tokenize

from datatrove.data import Document, DocumentsPipeline
from datatrove.io import BaseInputDataFolder, BaseOutputDataFolder, InputDataFile
from datatrove.pipeline.base import PipelineStep
from datatrove.utils.typeshelper import StatHints

from .utils import merge_docs, simplify_content, str_hash


@dataclass
class HashSig:
    hash_value: int
    doc_id: int
    sent_id: int
    file_id: int = None

    # priority queue accepts anything that is sortable
    def __lt__(self, other):
        return (self.hash_value, self.file_id, self.doc_id, self.sent_id) < (
            other.hash_value,
            other.file_id,
            other.doc_id,
            other.sent_id,
        )


class SentenceDedupSignature(PipelineStep):
    type = "🫂 - DEDUP"
    name = "💥 sentence-deduplication stage 1"

    def __init__(self, output_folder: BaseOutputDataFolder, n_sentences: int = 3, stage_2_workers: int = 1, **kwargs):
        super().__init__(**kwargs)
        self.output_folder = output_folder
        self.n_sentences = n_sentences
        self.stage_2_workers = stage_2_workers
        self.signatures = []

    def set_up_dl_locks(self, dl_lock, up_lock):
        self.output_folder.set_lock(up_lock)

    def save_hashes(self, rank: int):
        self.signatures.sort()

        f = self.output_folder.open(f"{rank:05d}.c4_sig", mode="wb")
        for hs in self.signatures:
            f.file_handler.write(struct.pack("<Q", hs.hash_value))
            f.file_handler.write(struct.pack("<I", hs.doc_id))
            f.file_handler.write(struct.pack("<H", hs.sent_id))
        self.output_folder.close()

    def get_hashes(self, doc: Document, doc_idx: int) -> list[None] | list[HashSig]:
        # todo use language id metadata in sent_tokenize
        sentences = sent_tokenize(doc.content)
        if len(sentences) < self.n_sentences:
            return []

        sentences_tokens = [simplify_content(sent) for sent in sentences]
        n_sent_grams: list = [
            " ".join(sentences_tokens[i : i + self.n_sentences])
            for i in range(len(sentences_tokens) - self.n_sentences + 1)
        ]
        hashes = [
            HashSig(
                hash_value=str_hash(n_sent_gram),
                doc_id=doc_idx,
                sent_id=sentence_idx,
            )
            for sentence_idx, n_sent_gram in enumerate(n_sent_grams)
        ]

        return hashes

    def __call__(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1):
        for doc_idx, doc in enumerate(data):
            self.stat_update(StatHints.total)
            self.signatures.extend(self.get_hashes(doc, doc_idx))
        self.save_hashes(rank)
        self.output_folder.close()


def read_sigs(file: InputDataFile, file_id: int) -> Generator[HashSig, None, None]:
    with file.open(binary=True) as f:
        while True:
            x = {}
            for t, b, k in [("Q", 8, "hash_value"), ("I", 4, "doc_id"), ("H", 2, "sent_id")]:
                by = f.read(b)
                if not by:
                    return
                x[k] = struct.unpack(f"<{t}", by)[0]
            yield HashSig(file_id=file_id, **x)


class SentenceFindDedups(PipelineStep):
    type = "🫂 - DEDUP"
    name = "💥 sentence-deduplication stage 2"

    def __init__(self, data_folder: BaseInputDataFolder, output_folder: BaseOutputDataFolder, **kwargs):
        super().__init__(**kwargs)
        self.data_folder = data_folder
        self.output_folder = output_folder

    def __call__(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1):
        sig_files = self.data_folder.list_files(".c4_sig")
        sig_readers = [read_sigs(file, file_i) for file_i, file in enumerate(sig_files)]

        pq = [next(sig_reader) for sig_reader in sig_readers]
        heapq.heapify(pq)

        last = None
        while pq:
            v: HashSig = heapq.heappop(pq)
            if last == v.hash_value:
                f = self.output_folder.open(f"{v.file_id:05d}.c4_dup", mode="wb")
                f.file_handler.write(struct.pack("<I", v.doc_id))
                f.file_handler.write(struct.pack("<H", v.sent_id))
            last = v.hash_value
            try:
                new_v = next(sig_readers[v.file_id])
            except StopIteration:
                new_v = None
            if new_v:
                heapq.heappush(pq, new_v)
        self.output_folder.close()


def read_dups(file: InputDataFile) -> Generator[tuple, None, None]:
    with file.open(binary=True) as f:
        while True:
            x = []
            for (
                t,
                b,
            ) in [("I", 4), ("H", 2)]:
                by = f.read(b)
                if not by:
                    return
                x.append(struct.unpack(f"<{t}", by)[0])
            yield tuple(x)


class SentenceDedupFilter(PipelineStep):
    type = "🫂 - DEDUP"
    name = "💥 sentence-deduplication stage 3"

    def __init__(
        self,
        data_folder: BaseInputDataFolder,
        min_doc_words: int = 50,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.data_folder = data_folder
        self.min_doc_words = min_doc_words

    def filter(self, doc: Document, du_lines: set = None):
        sentences = sent_tokenize(doc.content)
        # todo find a way to keep skip lines as in the original text
        doc.content = " ".join([sent for idx, sent in enumerate(sentences) if not du_lines or idx not in du_lines])
        if len(word_tokenize(doc.content)) > self.min_doc_words:
            return True
        return False

    def __call__(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        """
        step method for Filters.
        Drops documents that if .filter() is False

        @param datapipe: input DocumentsPipeline
        @return: DocumentsPipeline
        """
        files = self.data_folder.get_files_shard(rank, world_size)
        assert len(files) == 1
        du_file = merge_docs(sorted(read_dups(files[0])))
        for idx, doc in enumerate(data):
            self.stat_update(StatHints.total)
            with self.time_stats_manager:
                is_kept = self.filter(doc, du_lines=du_file.get(idx))
            if is_kept:
                yield doc
