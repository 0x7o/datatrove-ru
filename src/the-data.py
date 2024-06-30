import os

from datatrove.utils.typeshelper import Languages

from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    SafetyFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.pipeline.readers import WarcReader
from datatrove.pipeline.writers.jsonl import JsonlWriter

from argparse import ArgumentParser
from typing import List
import requests
import gzip
import nltk
import gc


def download_warc_paths(dump_to_process: str) -> List[str]:
    warc_url = f"https://data.commoncrawl.org/crawl-data/{dump_to_process}/warc.paths.gz"
    print(f"Downloading WARC paths for {dump_to_process}")
    response = requests.get(warc_url)
    return gzip.decompress(response.content).decode("utf-8").split("\n")


def download_warc_file(w: str) -> None:
    url = f"https://data.commoncrawl.org/{w}"
    file_name = w.split('/')[-1]
    file_path = f"cc/{file_name}"

    if os.path.exists(file_path):
        print(f"Skipping {file_name}")
        return

    print(f"Downloading {url}")
    r = requests.get(url)
    with open(file_path, "wb") as f:
        f.write(r.content)


def process_warc_batch(warc_paths: List[str], start: int, end: int) -> None:
    for i in range(start, min(end, len(warc_paths))):
        w = warc_paths[i]
        if not w.startswith("crawl-data"):
            continue
        download_warc_file(w)


def run(dump_to_process: str, main_output_path: str, host_id: int, total_hosts: int):
    FILTERING_OUTPUT_PATH = f"{main_output_path}/base_{host_id}"
    warc_paths = download_warc_paths(dump_to_process)

    total_files = len(warc_paths)
    batch_size = 48
    host_batch_size = batch_size * total_hosts

    batch_num = 0

    for batch_start in range(host_id * batch_size, total_files, host_batch_size):
        batch_end = batch_start + batch_size
        if not os.path.exists("cc"):
            os.makedirs("cc")
        print(f"Processing batch {batch_start // batch_size + 1} on host {host_id}")
        process_warc_batch(warc_paths, batch_start, batch_end)

        main_processing_executor = LocalPipelineExecutor(
            pipeline=[
                WarcReader(
                    # f"s3://commoncrawl/crawl-data/{DUMP_TO_PROCESS}/segments/",
                    "cc",
                    compression="gzip",
                    glob_pattern="*.warc.gz",
                ),
                URLFilter(
                    exclusion_writer=JsonlWriter(
                        f"{FILTERING_OUTPUT_PATH}/removed/1_url/{dump_to_process}"
                    )
                ),
                Trafilatura(timeout=5),
                LanguageFilter(
                    languages=(Languages.russian,),
                    language_threshold=0.75,
                    exclusion_writer=JsonlWriter(
                        f"{FILTERING_OUTPUT_PATH}/removed/2_non_russian",
                        output_filename="${language}/"
                                        + dump_to_process
                                        + "/${rank}.jsonl.gz",
                        # folder structure: language/dump/file
                    ),
                ),
                GopherRepetitionFilter(
                    exclusion_writer=JsonlWriter(
                        f"{FILTERING_OUTPUT_PATH}/removed/3_gopher_rep/{dump_to_process}"
                    ),
                    language=Languages.russian,
                ),
                GopherQualityFilter(
                    exclusion_writer=JsonlWriter(
                        f"{FILTERING_OUTPUT_PATH}/removed/4_gopher_qual/{dump_to_process}"
                    ),
                    language=Languages.russian,
                ),
                C4QualityFilter(
                    filter_no_terminal_punct=False,
                    exclusion_writer=JsonlWriter(
                        f"{FILTERING_OUTPUT_PATH}/removed/5_c4/{dump_to_process}"
                    ),
                    language=Languages.russian,
                ),
                FineWebQualityFilter(
                    exclusion_writer=JsonlWriter(
                        f"{FILTERING_OUTPUT_PATH}/removed/6_fineweb_qual/{dump_to_process}"
                    ),
                    language=Languages.russian,
                ),
                SafetyFilter(
                    exclusion_writer=JsonlWriter(
                        f"{FILTERING_OUTPUT_PATH}/removed/7_safety_filter/{dump_to_process}"
                    ),
                    model_name_or_path="apanc/russian-sensitive-topics",
                ),
                JsonlWriter(f"{FILTERING_OUTPUT_PATH}/output/{dump_to_process}"),
            ],
            tasks=128,
            workers=128,
            logging_dir=f"./logs/base_processing/{dump_to_process}",
            randomize_start_duration=180,  # don't hit the bucket all at once with the list requests
        )
        print(main_processing_executor.run())

        """
            we then applied minhash deduplication to each individual dump,
        """

        # remove all the files
        os.system(f"rm -rf cc")
        os.system(
            f"gsutil -m cp -r {FILTERING_OUTPUT_PATH}/output/{dump_to_process} {main_output_path}/{dump_to_process}/result_{host_id}/{batch_num}")
        os.system(f"gsutil -m rm -r {FILTERING_OUTPUT_PATH}")
        os.system(f"rm -rf ./logs")

        gc.collect()
        batch_num += 1


if __name__ == "__main__":
    nltk.download('punkt')
    parser = ArgumentParser()
    parser.add_argument("--dump", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--host", type=int, required=True)
    parser.add_argument("--total_hosts", type=int, required=True)
    args = parser.parse_args()
    run(args.dump, args.output, args.host, args.total_hosts)
