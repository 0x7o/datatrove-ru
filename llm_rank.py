from datasets import load_dataset, Dataset
from tqdm import tqdm
import requests
import time
import json
import os
import re


def get_score(text):
    r = requests.post(
        url="https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {os.environ.get('API_KEY')}",
        },
        data=json.dumps({
            "model": "meta-llama/llama-3-70b-instruct:nitro",
            "temperature": 0.0,
            "messages": [
                {"role": "user", "content": f"""Below is an extract from a web page. Evaluate whether the page has a high educational value and could be useful in an educational setting for teaching from primary school to grade school levels using the additive 5-point scoring system described below. Points are accumulated based on the satisfaction of each criterion:

- Add 1 point if the extract provides some basic information relevant to educational topics, even if it includes some irrelevant or non-academic content like advertisements and promotional material.
- Add another point if the extract addresses certain elements pertinent to education but does not align closely with educational standards. It might mix educational content with non-educational material, offering a superficial overview of potentially useful topics, or presenting information in a disorganized manner and incoherent writing style.
- Award a third point if the extract is appropriate for educational use and introduces key concepts relevant to school curricula. It is coherent though it may not be comprehensive or could include some extraneous information. It may resemble an introductory section of a textbook or a basic tutorial that is suitable for learning but has notable limitations like treating concepts that are too complex for grade school students.
- Grant a fourth point if the extract highly relevant and beneficial for educational purposes for a level not higher than grade school, exhibiting a clear and consistent writing style. It could be similar to a chapter from a textbook or a tutorial, offering substantial educational content, including exercises and solutions, with minimal irrelevant information, and the concepts aren't too advanced for grade school students. The content is coherent, focused, and valuable for structured learning.
- Bestow a fifth point if the extract is outstanding in its educational value, perfectly suited for teaching either at primary school or grade school. It follows detailed reasoning, the writing style is easy to follow and offers profound and thorough insights into the subject matter, devoid of any non-educational or complex content.

Do not add any point if the text contains errors or poor quality text.

The extract: {text[:512]}.

After examining the extract:

- Briefly justify your total score, up to 100 words.
- Conclude with the score using the format: "Educational score: <total points>"""}
            ]
        })
    ).json()["choices"][0]["message"]["content"]
    match = re.search(r'Educational score:\s*(\d+)', r)
    return int(match.group(1))


dataset = load_dataset("0x7o/the-data-2024-22-ru-sample")
data = {"text": [], "score": []}

i = 0

for item in tqdm(dataset["train"]):
    text = item["text"]
    finish = False
    time = 0
    while True:
        try:
            score = get_score(text)
            data["text"].append(text)
            data["score"].append(score)
            i += 1
            if i >= 4000:
                finish = True
            break
        except Exception as e:
            print(f"Error: {e}, sleeping {time} s.")
            time += 5
            continue
    if finish:
        break

dataset = Dataset.from_dict(data)
dataset.push_to_hub("0x7o/ru_rank")
