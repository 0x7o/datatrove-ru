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
        dup_n_grams: tuple[tuple[int, float]] = (
            (5, 0.15),
            (6, 0.14),
            (7, 0.13),
            (8, 0.12),
            (9, 0.11),
            (10, 0.10),
        ),
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
        if (
            self.dup_para_frac
            and paragraphs_duplicates / len(paragraphs) > self.dup_para_frac
        ):
            return False, "dup_para_frac"
        if (
            self.dup_para_char_frac
            and char_duplicates / len(text) > self.dup_para_char_frac
        ):
            return False, "dup_para_char_frac"

        lines = self._line_splitter.split(text)
        line_duplicates, char_duplicates = find_duplicates(lines)
        if self.dup_line_frac and line_duplicates / len(lines) > self.dup_line_frac:
            return False, "dup_line_frac"
        if (
            self.dup_line_char_frac
            and char_duplicates / len(text) > self.dup_line_char_frac
        ):
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
    text = 'Светильник для освещения СТО 55W 8928Lm IP65\nPT6M58StrueСветильник уличный накладной фасадный LED 5W 4200K IP65 Gurgen\nPT6M58StrueСвітильник виробничий ДСП Cobay 100W/150W/200W IP65 5000К\nPT6M58StrueСветильник светодиодный ДКУ-50W IP65 5600Лм 5000К Jazzway\nPT6M58StrueПрожектор LED VIDEX 100W 5000K ( лампа ,светильник,фонарь ).Светильники.570 UAH.\nPT6M58StrueSOLLA 2 Pack IP68 100W LED Flood Light Outdoor 5000K Daylight\nPT6M58StrueОбзор Светильник LED Smart 24W 2700-6500K 1920LM "Юта" IP20 170-265V / LM34007 +пульт, звездное небо\nPT6M58StrueКак подключить Светильник LED Smart 24W 2700-6500K 1920LM "Техас" IP20 170-265V +пульт\nPT6M58StrueСветодиодный промышленный подвесной светильник 100W IP65\nPT6M58StruePST-W S230080 3W 4000K GREY IP65 JazzWay\nPT6M58StrueСветильник Jazzway PWP-OS 1200 36w 6500K IP65\nPT6M58StrueСветодиодный прожектор 50W и 100W 6400K ONE LED IP65\nPT6M58StruePST W S230080 3W 4000K GREY IP65 JazzWay\nPT6M58StrueОбзор Led светильник ЖКХ 6W 6500K круг светодиодный IP65 Распаковка\nPT6M58StruePBH PC2 RS 12W 4000K IP65 SENSOR JazzWay\nPT6M58StrueShingel LED flood projetctor light RGBW 220V 50W 120lm/w CE/TUV IP65\nPT6M58Strue5000LM 50W IP66 LED spotlight with adjustable mounting - distributed by CABLEMATIC ®\nPT6M58StrueСветодиодный промышленный подвесной светильник 150W IP65\nЗдорово, что взяли именно эту модель. Послушайте опытных людей и будет вам счастье.\nИ как мы раньше обходились без этой классной модели 04 100w 5000k 13000lm ip65 5лет гар jazzway. Рекомендуем и вам! И по приемлимой стоимости.'
    doc = Document(text, id="test")
    filter_ = GopherRepetitionFilter(language=Languages.russian)
    print(filter_.filter(doc))
