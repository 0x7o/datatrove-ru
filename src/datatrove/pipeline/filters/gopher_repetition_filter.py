import re
from collections import Counter

from datatrove.data import Document
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.typeshelper import Languages
from datatrove.utils.word_tokenizers import load_word_tokenizer


"""
Table A1 from https://arxiv.org/pdf/2112.11446.pdf
    duplicate line fraction                 0.30
    duplicate paragraph fraction            0.30
    duplicate line character fraction       0.20
    duplicate paragraph character fraction  0.20

    top 2-gram character fraction           0.20
    top 3-gram character fraction           0.18
    top 4-gram character fraction           0.16

    duplicate 5-gram character fraction     0.15
    duplicate 6-gram character fraction     0.14
    duplicate 7-gram character fraction     0.13
    duplicate 8-gram character fraction     0.12
    duplicate 9-gram character fraction     0.11
    duplicate 10-gram character fraction    0.10
"""


def get_n_grams(words: list[str], n: int) -> list[str]:
    return [" ".join(words[i : i + n]) for i in range(len(words) - n + 1)]


def find_duplicates(x: list[str]) -> tuple[int, int]:
    unique_x = set()
    duplicate_chars = 0
    duplicate_elements = 0
    for element in x:
        if element in unique_x:
            duplicate_chars += len(element)
            duplicate_elements += 1

        else:
            unique_x.add(element)
    return duplicate_elements, duplicate_chars


def find_top_duplicate(x: list[str]) -> int:
    counter = Counter()
    for element in x:
        counter[element] += 1
    top_n_gram = counter.most_common(1)[0]
    return len(top_n_gram[0]) * top_n_gram[1]


def find_all_duplicate(words: list[str], n: int) -> int:
    n_words = len(words)
    unique = set()
    repeated_chars, idx = 0, 0
    while idx < n_words - n + 1:
        n_gram = "".join(words[idx : idx + n])
        if n_gram in unique:
            repeated_chars += len(n_gram)
            idx += n
        else:
            unique.add(n_gram)
            idx += 1
    assert repeated_chars <= len("".join(words))
    return repeated_chars


class GopherRepetitionFilter(BaseFilter):
    name = "👯 Gopher Repetition"

    def __init__(
        self,
        dup_line_frac: float | None = 0.3,
        dup_para_frac: float | None = 0.3,
        dup_line_char_frac: float | None = 0.2,
        dup_para_char_frac: float | None = 0.2,
        top_n_grams: tuple[tuple[int, float]] = ((2, 0.2), (3, 0.18), (4, 0.16)),
        dup_n_grams: tuple[tuple[int, float]] = ((5, 0.15), (6, 0.14), (7, 0.13), (8, 0.12), (9, 0.11), (10, 0.10)),
        exclusion_writer: DiskWriter = None,
        language: str = Languages.english,
    ):
        """

        Args:
            dup_line_frac:
            dup_para_frac:
            dup_line_char_frac:
            dup_para_char_frac:
            top_n_grams:
            dup_n_grams:
            exclusion_writer:
        """
        super().__init__(exclusion_writer)

        self.dup_line_frac = dup_line_frac
        self.dup_para_frac = dup_para_frac
        self.dup_line_char_frac = dup_line_char_frac
        self.dup_para_char_frac = dup_para_char_frac
        self.top_n_grams = top_n_grams
        self.dup_n_grams = dup_n_grams
        self.paragraph_exp = re.compile(r"\n{2,}")
        self._line_splitter = re.compile("\n+")
        self.tokenizer = load_word_tokenizer(language)

    def filter(self, doc: Document) -> bool | tuple[bool, str]:
        text = doc.text

        paragraphs = self.paragraph_exp.split(text.strip())
        paragraphs_duplicates, char_duplicates = find_duplicates(paragraphs)
        if self.dup_para_frac and paragraphs_duplicates / len(paragraphs) > self.dup_para_frac:
            return False, "dup_para_frac"
        if self.dup_para_char_frac and char_duplicates / len(text) > self.dup_para_char_frac:
            return False, "dup_para_char_frac"

        lines = self._line_splitter.split(text)
        line_duplicates, char_duplicates = find_duplicates(lines)
        if self.dup_line_frac and line_duplicates / len(lines) > self.dup_line_frac:
            return False, "dup_line_frac"
        if self.dup_line_char_frac and char_duplicates / len(text) > self.dup_line_char_frac:
            return False, "dup_line_char_frac"

        words = self.tokenizer.word_tokenize(text)

        for n, n_frac in self.top_n_grams:
            n_grams = get_n_grams(words, n)
            if not n_grams:
                continue
            top_char_length = find_top_duplicate(n_grams)
            if top_char_length / len(text) > n_frac:
                return False, f"top_{n}_gram"

        for n, n_frac in self.dup_n_grams:
            n_duplicates_char = find_all_duplicate(words, n)
            if n_duplicates_char / len(text) > n_frac:
                return False, f"duplicated_{n}_n_grams"

        return True


if __name__ == "__main__":
    text = 'Типовая смета на материал для ремонта квартиры - Здесь положено начало ремонта вашей квартиры Типовая смета на материал для ремонта квартиры Фотогалерея № 9287 посвященная темам:✔👍🏿 Купить Seaweed Organic Mask в Новосибирске развод Купить Seaweed Organic Mask в Новосибирске развод Елена Малышева: "Пигментация лица в Новосибирске осталась в прошлом". Перейти на сайт поставщика в Новосибирске Seaweed Organic Mask маска из водорослей › ✔✔✔ в Новосибирске ✔✔✔ Опубликовано: 09.09.2017, 20:12 | Автор: Дарина Матвеева Я тоже решила заказать.Когда глянула на статью, сразу поняла что Заказывала ЗДЕСЬ. Что этот тот же сайт. О котором и врач говорил. Вот только сроки до эффекта у меня другие, не как у Елены, не знаю с чем это может быть связано.Кожа очистилась уж точно не за месяц, точно помню, пигментация была в разы сильнее, так что нет, не месяц. А примерно 6 недель. Зато результата стоило ждать, он просто невероятен. Хотя в маске ходить конечно странновато, выгляжу странно, мужа запугивала довольно долго)))'
    doc = Document(text, id="test")
    filter_ = GopherRepetitionFilter(language=Languages.russian)
    print(filter_.filter(doc))
