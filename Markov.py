# TODO I think that the generating can be spead up by having a better
# key value store so lets work on that

from SwankinsStorage import Storage, MessageRecord,\
                            WordPairRecord, SourceRecord
import logging

_TRANSLATION_TABLE = str.maketrans({
    # '.':'',
    # '?':'',
    # ' ': '',
    ',': '',
    '"': '',
    ':': '',
    ';': '',
    "'": '',
    '-': ' ',
    '\n': ' ',
    '\t': ' ',
})


class Markov():
    """The chain generating part of the program!"""

    def __init__(self, storage=None, sources=None):
        if not isinstance(storage, Storage):
            pass  # TODO Throw an error
        self.storage = storage
        self.sources = sources
        self._read_sources(sources)

    def messages(self):
        for chain in self._chains():
            self.storage.store(MessageRecord(message=chain))
            yield chain

    def _stop_condition(self, words):
        if len(words) > 130:
            return True
        return False

    def _get_starting_word(self):
        return self.storage.get_starting_word()

    def _get_next_word(self, word):
        return self.storage.get_next_word(word)

    def _chains(self):
        while True:
            word = self._get_starting_word()
            chain = (word + ' ')

            while not self._stop_condition(chain):
                word = self._get_next_word(word)
                chain += (word + ' ')

            # [:-1] to remove the space at the end
            yield chain.capitalize()[:-1]

    def _break_file(self, filename):
        with open(filename, 'r') as fin:
            source = fin.read()

            words = [x for x in source.translate(_TRANSLATION_TABLE).split(' ')
                     if x is not ' ' and x is not '']

            del(source)  # Free the file asap

        return words

    def _fetch_pair(self, filename):
        words = self._break_file(filename)

        for i in range(0, len(words) - 1):
            key = words[i]
            val = words[i + 1]
            yield key, val

    def _read_source(self, filename):
        pairs = WordPairRecord()

        for key, val in self._fetch_pair(filename):
            pairs.add(this=key, next=val)

        self.storage.store(pairs)

    # TODO this will be the MP function it just needs to distribute and join
    # the information to and from _read_sources
    def _read_sources(self, filenames):
        scanned_sources = self.storage.get_sources()

        for file in filenames:
            if file not in scanned_sources:
                self._read_source(file)
                self.storage.store(SourceRecord(source_name=file))
                logging.debug(f'Read [{file}]')
