import random
from collections import defaultdict

_TRANSLATION_TABLE = str.maketrans({
    '.':'',
    # '?':'',
    # ' ': '',
    ',': '',
    '(': '',
    ')': '',
    '{': '',
    '}': '',
    '[': '',
    ']': '',
    '|': '',
    '"': '',
    ':': '',
    ';': '',
    "'": '',
    '/': '',
    '_': '',
    '\\': '',
    '-': ' ',
    '\n': ' ',
    '\t': ' ',
})


class Markov():
    """The chain generating part of the program!"""

    def __init__(self, sources=None):
        self.memory = defaultdict(list)
        self.sources = sources
        self._read_sources(sources)

    def messages(self):
        for chain in self._chains():
            yield chain

    def _stop_condition(self, words):
        if len(words) > 130:
            return True
        return False

    def _get_starting_word(self):
        return random.choice(list(self.memory.keys()))

    def _get_next_word(self, word):
        if len(self.memory[word]) == 0:
            return None
        else:
            return random.choice(self.memory[word])

    def _chains(self):
        while True:
            word = self._get_starting_word()
            chain = word + ' '

            while not self._stop_condition(chain):
                word = self._get_next_word(word)

                if word is None:
                    break

                chain += word + ' '

            # [:-1] to remove the space at the end
            yield chain.capitalize()[:-1]

    def _break_file(self, filename):
        with open(filename, 'r') as fin:
            source = fin.read()

            words = [word for word in source.translate(_TRANSLATION_TABLE).split(' ')
                     if word is not ' ' and word is not '']

        return words

    def _fetch_pair(self, filename):
        words = self._break_file(filename)

        for i in range(0, len(words) - 1):
            key = words[i]
            val = words[i + 1]
            yield key, val

    def _read_source(self, filename):
        for key, val in self._fetch_pair(filename):
            self.memory[key] += [val]

    def _read_sources(self, filenames):
        for file in filenames:
            self._read_source(file)

