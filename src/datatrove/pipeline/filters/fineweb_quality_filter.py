from datatrove.data import Document
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.filters.gopher_repetition_filter import find_duplicates
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.typeshelper import Languages
from datatrove.utils.word_tokenizers import load_word_tokenizer


class FineWebQualityFilter(BaseFilter):
    name = "🍷 FineWeb Quality"

    def __init__(
            self,
            exclusion_writer: DiskWriter = None,
            line_punct_thr: float = 0.12,
            line_punct_exclude_zero: bool = False,
            short_line_thr: float = 0.67,
            short_line_length: int = 30,
            char_duplicates_ratio: float = 0.01,
            new_line_ratio: float = 0.3,
            language: str = Languages.english,
    ):
        super().__init__(exclusion_writer)
        self.line_punct_thr = line_punct_thr
        self.line_punct_exclude_zero = line_punct_exclude_zero
        self.short_line_threshold = short_line_thr
        self.short_line_length = short_line_length
        self.char_duplicates_ratio = char_duplicates_ratio
        self.new_line_ratio = new_line_ratio
        self.tokenizer = load_word_tokenizer(language)

    def filter(self, doc) -> bool | tuple[bool, str]:
        stop_chars = (".", "'", '"', "!", "?")

        lines = doc.text.split("\n")
        ratio = sum(1 for line in lines if line.endswith(stop_chars)) / len(lines)
        if ratio <= self.line_punct_thr and not (ratio == 0 and self.line_punct_exclude_zero):
            return False, "line_punct_ratio"

        ratio = sum(1 for line in lines if len(line) <= self.short_line_length) / len(lines)
        if ratio >= self.short_line_threshold:
            return False, "short_line_ratio"

        non_empty_lines = [line for line in lines if line.strip() != ""]
        ratio = find_duplicates(non_empty_lines)[1] / len(doc.text.replace("\n", ""))

        if ratio >= self.char_duplicates_ratio:
            return False, "char_dup_ratio"

        words = self.tokenizer.word_tokenize(doc.text)
        new_line = doc.text.count("\n")
        if new_line / len(words) > self.new_line_ratio:
            return False, "list_ratio"

        return True


if __name__ == "__main__":
    filter = FineWebQualityFilter(language=Languages.russian)
    doc = Document(text='✔👍🏿 Купить Seaweed Organic Mask в Новосибирске развод Купить Seaweed Organic Mask в Новосибирске развод Елена Малышева: "Пигментация лица в Новосибирске осталась в прошлом". Перейти на сайт поставщика в Новосибирске Seaweed Organic Mask маска из водорослей › ✔✔✔ в Новосибирске ✔✔✔ Опубликовано: 09.09.2017, 20:12 | Автор: Дарина Матвеева Я тоже решила заказать.Когда глянула на статью, сразу поняла что Заказывала ЗДЕСЬ. Что этот тот же сайт. О котором и врач говорил. Вот только сроки до эффекта у меня другие, не как у Елены, не знаю с чем это может быть связано.Кожа очистилась уж точно не за месяц, точно помню, пигментация была в разы сильнее, так что нет, не месяц. А примерно 6 недель. Зато результата стоило ждать, он просто невероятен. Хотя в маске ходить конечно странновато, выгляжу странно, мужа запугивала довольно долго)))', id="1")
    print(filter.filter(doc))
