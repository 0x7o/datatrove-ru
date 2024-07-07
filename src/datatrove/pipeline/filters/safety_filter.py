from datatrove.data import Document
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.writers.disk_base import DiskWriter


class SafetyFilter(BaseFilter):
    name = "🔞 Safety Filter"
    _requires_dependencies = ["transformers"]

    def __init__(self, model_name_or_path: str, exclusion_writer: DiskWriter = None):
        from transformers import AutoTokenizer, AutoModelForSequenceClassification

        super().__init__(exclusion_writer)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name_or_path)
        self.model = AutoModelForSequenceClassification.from_pretrained(
            model_name_or_path
        )
        self.model.eval()
        self.bad = "2 3 4 5 6 7 8 9 14".split(" ")

    def filter(self, doc: Document) -> bool:
        """Args:
            doc: document

        Returns:
            is_filter
        """
        inputs = self.tokenizer(
            doc.text, return_tensors="pt", padding=True, truncation=True, max_length=128
        )
        logits = self.model(**inputs).logits
        predicted_class_id = logits.argmax().item()
        result = self.model.config.id2label[predicted_class_id]
        label = result.split("_")[-1]
        if label in self.bad and result["score"] > 0.8:
            return False
        return True


if __name__ == "__main__":
    s = SafetyFilter(model_name_or_path="0x7o/rubert-tiny-sensitive-topics")
