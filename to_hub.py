from datasets import Dataset
import json

file = "CC-MAIN-2024-22_result_1_00003.jsonl"
dataset = {"text": [], "url": [], "date": []}

with open(file, "r", encoding="utf-8") as f:
    for line in f:
        data = json.loads(line)
        dataset["text"].append(data["text"])
        dataset["url"].append(data["metadata"]["url"])
        dataset["date"].append(data["metadata"]["date"])

dataset = Dataset.from_dict(dataset)
dataset = dataset.shuffle()
dataset.push_to_hub("0x7o/the-data-2024-22-ru-sample")
