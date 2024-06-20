from datasets import Dataset
import gcsfs
import gzip
import json

fs = gcsfs.GCSFileSystem(project='utility-zenith-421002')
files = fs.ls('ae-the-data/CC-MAIN-2024-22/result_1')

dataset = {"text": [], "url": [], "date": []}

for file in files:
    with fs.open(file, "rb") as f:
        with gzip.GzipFile(fileobj=f) as fr:
            for line in fr:
                data = json.loads(line.decode('utf-8'))
                dataset["text"].append(data["text"])
                dataset["url"].append(data["metadata"]["url"])
                dataset["date"].append(data["metadata"]["date"])

dataset = Dataset.from_dict(dataset)
dataset = dataset.shuffle()
dataset.push_to_hub("0x7o/the-data-2024-22-ru-sample")
