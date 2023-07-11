from datatrove.pipeline.base import DocumentsPipeline, PipelineStep


class Nerdino(PipelineStep):
    type = "📊 - STATS"
    name = "🤓 document length"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __call__(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        for doc in data:
            with self.stats.time_manager:
                self.stats.doc_len.update(len(doc.content))
            yield doc
