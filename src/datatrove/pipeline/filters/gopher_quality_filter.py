import numpy as np

from datatrove.data import Document
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.text import PUNCTUATION_SET
from datatrove.utils.typeshelper import Languages
from datatrove.utils.word_tokenizers import load_word_tokenizer

STOP_WORDS = ['от', 'конечно', 'ей', 'можно', 'вам', 'так', 'почти', 'тем', 'будет', 'их', 'нее', 'моя', 'всех', 'него',
              'совсем', 'во', 'другой', 'бы', 'им', 'не', 'про', 'же', 'на', 'или', 'она', 'раз', 'кто', 'нет', 'много',
              'более', 'куда', 'хоть', 'тоже', 'чего', 'нас', 'мы', 'тогда', 'до', 'тут', 'разве', 'перед', 'этом',
              'лучше', 'нельзя', 'себе', 'какой', 'чтобы', 'какая', 'но', 'ну', 'меня', 'чтоб', 'когда', 'три', 'тебя',
              'где', 'надо', 'без', 'уже', 'вас', 'вдруг', 'может', 'была', 'ни', 'еще', 'потом', 'больше', 'он',
              'никогда', 'для', 'мой', 'мне', 'через', 'вот', 'был', 'ведь', 'после', 'ней', 'если', 'были', 'опять',
              'ли', 'об', 'сейчас', 'этого', 'сам', 'свою', 'со', 'иногда', 'только', 'чем', 'ему', 'впрочем', 'всю',
              'потому', 'один', 'из', 'уж', 'они', 'нибудь', 'да', 'них', 'чуть', 'будто', 'себя', 'быть', 'между',
              'его', 'эту', 'хорошо', 'есть', 'эти', 'этот', 'при', 'все', 'даже', 'того', 'по', 'ним', 'как', 'зачем',
              'два', 'такой', 'теперь', 'там', 'над', 'вы', 'здесь', 'тот', 'всегда', 'ты', 'наконец', 'этой', 'ее',
              'что', 'ничего', 'всего', 'том', 'под', 'за', 'то', 'было']


class GopherQualityFilter(BaseFilter):
    name = "🥇 Gopher Quality"

    def __init__(
            self,
            min_doc_words: int | None = 50,
            max_doc_words: int | None = 100000,
            min_avg_word_length: int | None = 3,
            max_avg_word_length: int | None = 10,
            max_symbol_word_ratio: float | None = 0.1,
            max_bullet_lines_ratio: float | None = 0.9,
            max_ellipsis_lines_ratio: float | None = 0.3,
            max_non_alpha_words_ratio: float | None = 0.8,
            min_stop_words: int | None = 2,
            stop_words: list[str] | None = None,
            exclusion_writer: DiskWriter = None,
            language: str = Languages.english,
    ):
        """
        Filter to apply Gopher's quality heuristic rules.
        Reference: https://arxiv.org/pdf/2112.11446.pdf

        Args:
            min_doc_words:
            max_doc_words:
            min_avg_word_length:
            max_avg_word_length:
            max_symbol_word_ratio:
            max_bullet_lines_ratio:
            max_ellipsis_lines_ratio:
            max_non_alpha_words_ratio:
            min_stop_words:
            stop_words:
            exclusion_writer:
        """
        super().__init__(exclusion_writer)
        self.min_doc_words = min_doc_words
        self.max_doc_words = max_doc_words
        self.min_avg_word_length = min_avg_word_length
        self.max_avg_word_length = max_avg_word_length
        self.max_symbol_word_ratio = max_symbol_word_ratio
        self.max_bullet_lines_ratio = max_bullet_lines_ratio
        self.max_ellipsis_lines_ratio = max_ellipsis_lines_ratio
        self.max_non_alpha_words_ratio = max_non_alpha_words_ratio
        self.min_stop_words = min_stop_words
        self.stop_words = set(STOP_WORDS if stop_words is None else stop_words)
        self.tokenizer = load_word_tokenizer(language)

    def filter(self, doc: Document) -> bool | tuple[bool, str]:
        """

        Args:
            doc: Applies the heuristics rules to decide if a document should be REMOVED


        Returns: False if sample.text does not pass any of the the heuristic tests

        """
        text = doc.text
        words = self.tokenizer.word_tokenize(text)
        n_words = len(words)

        non_symbol_words = [w for w in words if any(ch not in PUNCTUATION_SET for ch in w)]
        n_non_symbol_words_words = len(non_symbol_words)

        # words < min_doc_words or words > max_doc_words
        if self.min_doc_words and n_non_symbol_words_words < self.min_doc_words:
            return False, "gopher_short_doc"
        if self.max_doc_words and n_non_symbol_words_words > self.max_doc_words:
            return False, "gopher_long_doc"

        # mean word length is outside the range of 3 to 10 characters
        avg_n_words = np.mean([len(w) for w in non_symbol_words])
        if self.min_avg_word_length and avg_n_words < self.min_avg_word_length:
            return False, "gopher_below_avg_threshold"
        if self.max_avg_word_length and avg_n_words > self.max_avg_word_length:
            return False, "gopher_above_avg_threshold"

        # symbol-to-word ratio greater than 0.1 for either the hash symbol or the ellipsis
        if self.max_symbol_word_ratio and text.count("#") / n_words > self.max_symbol_word_ratio:
            return False, "gopher_too_many_hashes"
        if self.max_symbol_word_ratio and (text.count("...") + text.count("…")) / n_words > self.max_symbol_word_ratio:
            return False, "gopher_too_many_ellipsis"

        # any document with more than 90 % of lines starting with a bullet point,
        # or more than 30 % ending with an ellipsis.
        lines = text.splitlines()
        if (
                self.max_bullet_lines_ratio
                and sum(s.lstrip().startswith("•") or s.lstrip().startswith("-") for s in lines) / len(lines)
                > self.max_bullet_lines_ratio
        ):
            return False, "gopher_too_many_bullets"
        if (
                self.max_ellipsis_lines_ratio
                and sum(s.rstrip().endswith("...") or s.rstrip().endswith("…") for s in lines) / len(lines)
                > self.max_ellipsis_lines_ratio
        ):
            return False, "gopher_too_many_end_ellipsis"

        # that 80 % of words in a document contain at least one alphabetic character
        if (
                self.max_non_alpha_words_ratio
                and sum([any((c.isalpha() for c in w)) for w in words]) / n_words < self.max_non_alpha_words_ratio
        ):
            return False, "gopher_below_alpha_threshold"

        # stop word filter
        if self.min_stop_words and sum(w in self.stop_words for w in words) < self.min_stop_words:
            return False, "gopher_enough_stop_words"

        return True


if __name__ == "__main__":
    filter = GopherQualityFilter(
        language=Languages.russian,
    )
    doc = Document(
        id="test",
        text="""Уборка в армии - Страница 3 - Форум - Мать солдата Рада33 Понедельник, 19.03.2018, 17:11 | Сообщение № 31 | Цитата | Ответить Да их нигде уж нету, тю-тю! Сами солдатики пол драют. Причем, втирают нам, мол им специальные пылесосы закупили... Не знаю, в нашей все ручками, кучу времени на ерунду эту тратят! А зимой снег разгребать с лопатами посылали - а коммунальщики за что денюжки свои получают??? Чтоб свою работу на крайнего переложить, вот-вот! Марина_70 Среда, 21.03.2018, 10:18 | Сообщение № 32 | Цитата | Ответить Я вот тоже не понимаю что это за служба такая? Ведь не уборке учиться идут ребята в армию, а служить стране. ЕленаПремудрая Пятница, 06.04.2018, 15:52 | Сообщение № 33 | Цитата | Ответить Пока всё говорит, просто кричит об обратном. Похоже, наши мальчишки были призваны в войска уборщиков. И защищать страну их учат с помощью совков и метёлок! Увы. masha233 Четверг, 12.04.2018, 11:15 | Сообщение № 34 | Цитата | Ответить мой сын рассказывал, как зимой снег каждый день заставляли чистить, по несколько часов на морозе. вот служба-то отличная...нашли рабочую силу. мне кажется, каждый должен заниматься своим делом. а у нас как всегда... IrinaVas Понедельник, 23.04.2018, 11:58 | Сообщение № 35 | Цитата | Ответить И не говорите...зимой был снег, так они его и расчищали. А теперь все тает и они одной рукой вычерпывают талую воду , а другой убирают прошлогодний мусор, который вылез из под снега как подснежники...а где коммунальные службы городов? почему привлекают солдатов не ясно... Andry Понедельник, 07.05.2018, 10:02 | Сообщение № 36 | Цитата | Ответить Цитата ЕленаПремудрая ( ) Похоже, наши мальчишки были призваны в войска уборщиков. И защищать страну их учат с помощью совков и метёлок! Увы. К сожалению так и есть! Вот только полы бы они и на гражданке могли бы драить... masha233 Четверг, 10.05.2018, 11:05 | Сообщение № 37 | Цитата | Ответить обсуждать и ругаться - это все хорошо. вот только это не даст ничего, надо делать что-то для того, чтобы исправить ситуацию. я написала в общественную организацию, жду ответа Andry Пятница, 18.05.2018, 15:55 | Сообщение № 38 | Цитата | Ответить Аналогичная ситуация. Ежедневно со шваброй и метлой в руках. Складывается такое впечатление, что автомат теперь в армии не обязателен. Я, конечно, помню ПХД раньше, но и боевая подготовка при этом была нормальная. А сейчас приходят оттуда и говорят, что оружие в руках держали всего пару раз. Зато метлу - каждый день. Это неправильно. GhostPetor Вторник, 22.05.2018, 10:53 | Сообщение № 39 | Цитата | Ответить Правильно выше написали, что нужно обращаться в компетентные инстанции. Просто жалобами здесь мы вряд ли что-то изменим. И, я так понимаю, что такая ситуация сейчас практически везде. Комитеты солдатских матерей тут бессильны? IrinaVas Пятница, 01.06.2018, 11:59 | Сообщение № 40 | Цитата | Ответить А сейчас приходят оттуда и говорят, что оружие в руках держали всего пару раз. Зато метлу - каждый день. Это неправильно. Полностью поддерживаю! Армия для того и армия, чтобы научиться защищать страну и семью! А не с метлой и тряпками бегать Andry Вторник, 05.06.2018, 10:35 | Сообщение № 41 | Цитата | Ответить Цитата IrinaVas ( ) Армия для того и армия, чтобы научиться защищать страну и семью! А не с метлой и тряпками бегать Полностью соглашусь. Реально с тряпкой и метлой можно на гражданке упражняться, если есть желание. А в армию нужно идти служить, заниматься физической подготовкой, осваивать оружие... Папаша Среда, 06.06.2018, 19:47 | Сообщение № 42 | Цитата | Ответить Цитата GhostPetor ( ) Да. Такая ситуация сейчас везде. Лет 5-7 назад, в воинских частях (не во всех) были гражданские уборщики. Но после кризиса 2014 года, гражданские уборщики, незаметно исчезли из воинских частей. ЕленаПремудрая Пятница, 20.07.2018, 09:20 | Сообщение № 43 | Цитата | Ответить Столько читаю о том. Как в армии всё меняется к лучшему, особенно бытовые условия, душевые там, звонить вот теперь можно, питание вкусное, но неужели нельзя сделать и с уборкой что-то? Это же невозможно, сын как не позвонит, всегда «я ненадолго, мам», т.к. он или только с уборки, или бежит на уборку. Мозоли говорит не проходят на руках, то мётлами машут, то лопатами … Чему они так научатся в этой армии?? IrinaVas Вторник, 24.07.2018, 09:49 | Сообщение № 44 | Цитата | Ответить Комитеты стараются вроде что-то сделать, но некоторым мамам пусть лучше сынок машет тряпкой, чей оружие в руки берет - не дай бог что. По мне так мужик должен быть мужиком, и не бояться таких вещей...всю жизнь держась за материнский подол жить не получится ElenaSviridova Вторник, 04.09.2018, 10:35 | Сообщение № 45 | Цитата | Ответить Уборка уборкой, но не большую часть времени же? Сын вроде в танковых, а по сути - тряпко-ведерных войсках. Добавлено (11.10.2018, 12:19) Столько читаю о том. Как в армии всё меняется к лучшему, особенно бытовые условия, душевые там, звонить вот теперь можно, питание вкусное, но неужели нельзя сделать и с уборкой что-то? Это же невозможно, сын как не позвонит, всегда «я ненадолго, мам», т.к. он или только с уборки, или бежит на уборку. Мозоли говорит не проходят на руках, то мётлами машут, то лопатами … Чему они так научатся в этой армии?? >( Не все коту Масленица...уборка уборкой, но все-таки сын мечтал потом пойти по военной стезе. А с такими навыками далеко не уйдешь, конечно"""
    )
    print(filter.filter(doc))
