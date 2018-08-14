import sqlite3
import logging

_SQL_TRANSLATION_TABLE = str.maketrans({
    "'": "''",
})


class Storage():
    """The storage interface for swankins"""

    def __init__(self, filename=None):
        self.filename = filename
        self.conn = sqlite3.connect(filename)

        logging.debug(f'Database connected at {filename}')

        c = self.conn.cursor()
        tables_sql = ['CREATE TABLE IF NOT EXISTS Sources (\
        Name TINYTEXT NOT NULL PRIMARY KEY\
        );', 'CREATE TABLE IF NOT EXISTS Tweets (\
        Status TINYTEXT NOT NULL,\
        TweetDate DEFAULT CURRENT_TIMESTAMP,\
        PRIMARY KEY (Status, TweetDate)\
        );', 'CREATE TABLE IF NOT EXISTS Words (\
        ID INTEGER PRIMARY KEY,\
        This TINYTEXT NOT NULL,\
        Next TINYTEXT NOT NULL\
        );', 'CREATE TABLE IF NOT EXISTS Quotes (\
        Quote TEXT NOT NULL PRIMARY KEY\
        )']

        for sql in tables_sql:
            c.execute(sql)
        self.conn.commit()
        c.close()
        logging.debug('Tables created')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def store(self, record):
        if not isinstance(record, Record):
            pass  # TODO Throw an error here

        c = self.conn.cursor()
        sql = record.sql()
        logging.debug(f'Running [{sql}]')
        c.execute(sql)
        self.conn.commit()
        c.close()

    # TODO all these functions here that are used by Markov.py may be removed
    # for a more proformate key-value store
    def get_message(self):
        c = self.conn.cursor()

        sql = 'SELECT Text FROM Messages ORDER BY RANDOM() LIMIT 1'
        c.execute(sql)
        self.conn.commit()
        ((res, ), ) = c.fetchall()
        c.close()
        return res

    def get_starting_word(self):
        c = self.conn.cursor()

        sql = 'SELECT This FROM Words ORDER BY RANDOM() LIMIT 1'
        c.execute(sql)
        self.conn.commit()

        ((res, ), ) = c.fetchall()
        c.close()
        return res

    def get_next_word(self, word):
        c = self.conn.cursor()

        sql = f'SELECT Next FROM Words\
                WHERE This = "{word}" ORDER BY RANDOM() LIMIT 1'
        c.execute(sql)
        self.conn.commit()

        ((res, ), ) = c.fetchall()
        c.close()
        return res

    def get_sources(self):
        c = self.conn.cursor()

        c.execute('SELECT * FROM Sources')
        self.conn.commit()
        res = [x for (x, ) in c.fetchall()]
        c.close()
        logging.debug(f'Sources [{res}]')
        return res


# TODO Subclass the record type and have those deal with the sql that they
# need to generate to store themselves in the database instead of having this
# RecordType Enum that requires a bunch of if statments to process
class Record():
    """The base record object
    TODO Write something about sub classing this to implement a record"""

    def __init__(self):
        pass  # TODO raise an error here because this needs to be subclassed

    def sql(self):
        pass


class TweetRecord(Record):
    """The message record for Twitter status updates"""

    def __init__(self, status=None):
        if status is None:
            pass  # TODO throw an error

        # TODO This is where I would generate more metadata if I wanted
        self.status = status.translate(_SQL_TRANSLATION_TABLE)

    def sql(self):
        return f"INSERT INTO Tweets (Status) VALUES ('{self.status}')"


class WordPairRecord(Record):
    """The record used to store word pairs"""

    def __init__(self, this=None, next=None):
        self.query = 'INSERT INTO Words (This, Next) VALUES'

    def add(self, this=None, next=None):
        if this is None or next is None:
            pass  # TODO throw an error

        self.query += f'("{this}", "{next}"),'

    def sql(self):
        return self.query[:-1]


class SourceRecord(Record):
    """The record class to store sources"""

    def __init__(self, source_name=None):
        if source_name is None:
            pass  # TODO throw an error
        self.source_name = source_name.translate(_SQL_TRANSLATION_TABLE)

    def sql(self):
        return f"INSERT INTO Sources (Name) VALUES ('{self.source_name}')"


class MessageRecord(Record):
    """The record class to store the markov messages"""

    def __init__(self, message=None):
        if message is None:
            pass  # TODO throw an error
        self.message = message.translate(_SQL_TRANSLATION_TABLE)

    def sql(self):
        return f"INSERT INTO Quotes (Quote) VALUES ('{self.message}')"
