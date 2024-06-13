from datatrove.utils.typeshelper import Languages

from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.dedup import MinhashDedupCluster, MinhashDedupFilter, MinhashDedupSignature
from datatrove.pipeline.dedup.minhash import MinhashConfig, MinhashDedupBuckets
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.pipeline.formatters import PIIFormatter
from datatrove.pipeline.readers import JsonlReader, WarcReader
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.writers.jsonl import JsonlWriter

"""
    we first ran the following pipeline for each dump
"""
DUMP_TO_PROCESS = "CC-MAIN-2023-50"  # example

MAIN_OUTPUT_PATH = "gs://ae-the-data"
FILTERING_OUTPUT_PATH = f"{MAIN_OUTPUT_PATH}/base_processing"


def run():
    main_processing_executor = LocalPipelineExecutor(
        pipeline=[
            WarcReader(
                # f"s3://commoncrawl/crawl-data/{DUMP_TO_PROCESS}/segments/",
                "cc",
                compression="gzip",
                glob_pattern="*.warc.gz",
            ),
            URLFilter(exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/1_url/{DUMP_TO_PROCESS}")),
            Trafilatura(favour_precision=True),
            LanguageFilter(
                languages=(Languages.russian,),
                exclusion_writer=JsonlWriter(
                    f"{FILTERING_OUTPUT_PATH}/2_non_russian/",
                    output_filename="${language}/" + DUMP_TO_PROCESS + "/${rank}.jsonl.gz",
                    # folder structure: language/dump/file
                )
            ),
            GopherRepetitionFilter(
                exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/3_gopher_rep/{DUMP_TO_PROCESS}"),
                language=Languages.russian
            ),
            GopherQualityFilter(
                exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/4_gopher_qual/{DUMP_TO_PROCESS}"),
                language=Languages.russian
            ),
            C4QualityFilter(
                filter_no_terminal_punct=False,
                exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/5_c4/{DUMP_TO_PROCESS}"),
                language=Languages.russian
            ),
            FineWebQualityFilter(
                exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/6_fineweb_qual/{DUMP_TO_PROCESS}"),
                language=Languages.russian
            ),
            JsonlWriter(f"{FILTERING_OUTPUT_PATH}/output/{DUMP_TO_PROCESS}"),
        ],
        tasks=4,
        workers=16,
        logging_dir=f"./logs/base_processing/{DUMP_TO_PROCESS}",
        randomize_start_duration=180,  # don't hit the bucket all at once with the list requests
    )
    print(main_processing_executor.run())


if __name__ == "__main__":
    run()
#
# """
#     we then applied minhash deduplication to each individual dump,
# """
#
# # you can also change ngrams or the number of buckets and their size here
# minhash_config = MinhashConfig(
#     num_buckets=14,
#     hashes_per_bucket=8,
#     n_grams=5,
# )
#
# S3_MINHASH_BASE_PATH = f"{MAIN_OUTPUT_PATH}/minhash"
#
# S3_LOGS_FOLDER = f"{MAIN_OUTPUT_PATH}/logs/minhash"
# LOCAL_LOGS_FOLDER = "logs/minhash"
#
# TOTAL_TASKS = 1000
#
# # this is the original data that we want to deduplicate
# INPUT_READER = JsonlReader(
#     f"{FILTERING_OUTPUT_PATH}/output/{DUMP_TO_PROCESS}"
# )  # this is the output from the first part
#
# # stage 1 computes minhash signatures for each task (each task gets a set of files)
# stage1 = LocalPipelineExecutor(
#     pipeline=[
#         INPUT_READER,
#         MinhashDedupSignature(
#             output_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/signatures", config=minhash_config, language=Languages.russian
#         ),
#     ],
#     tasks=TOTAL_TASKS,
#     logging_dir=f"{S3_LOGS_FOLDER}/signatures",
#     randomize_start_duration=180,
#     depends=main_processing_executor,  # only start after the first one completes
# )
#
# stage2 = LocalPipelineExecutor(
#     pipeline=[
#         MinhashDedupBuckets(
#             input_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/signatures",
#             output_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/buckets",
#         ),
#     ],
#     tasks=minhash_config.num_buckets * 50,  # the code supports parallelizing each bucket. here we run 50
#     # workers per bucket
#     randomize_start_duration=180,
#     logging_dir=f"{S3_LOGS_FOLDER}/buckets",
#     depends=stage1,
# )
#
# stage3 = LocalPipelineExecutor(
#     pipeline=[
#         MinhashDedupCluster(
#             input_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/buckets",
#             output_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/remove_ids",
#             config=minhash_config,
#         ),
#     ],
#     tasks=1,  # this step runs on a single task
#     logging_dir=f"{S3_LOGS_FOLDER}/clustering",
#     depends=stage2,
# )
#
# stage4 = LocalPipelineExecutor(
#     pipeline=[
#         INPUT_READER,
#         TokensCounter(tokenizer_name_or_path="aeonium/Aeonium-v1.1-Base-4B"),
#         # before and after dedup
#         MinhashDedupFilter(input_folder=f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/remove_ids"),
#         # run the PII removal
#         PIIFormatter(),
#         JsonlWriter(f"{S3_MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/deduped_output"),
#     ],
#     tasks=TOTAL_TASKS,
#     logging_dir=f"{S3_LOGS_FOLDER}/filtering",
#     depends=stage3,
# )
#
# # launch dedup pipelines
# stage4.run()
