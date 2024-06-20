from datasets import Dataset
import gcsfs
import json

fs = gcsfs.GCSFileSystem(project='utility-zenith-421002')
files = fs.ls('ae-the-data/CC-MAIN-2024-22/result_1')

dataset = {"text": [], "url": [], "date": []}

for file in files:
    with fs.open(f"ae-the-data/CC-MAIN-2024-22/result_1/{file}", "r", encoding="utf-8") as f:
        for line in f:
            data = json.loads(line)
            dataset["text"].append(data["text"])
            dataset["url"].append(data["metadata"]["url"])
            dataset["date"].append(data["metadata"]["date"])

dataset = Dataset.from_dict(dataset)
dataset = dataset.shuffle()
dataset.push_to_hub("0x7o/the-data-2024-22-ru-sample")
