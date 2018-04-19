#!/usr/bin/env python3

import sqlite3
import os
import os.path

DB_NAME = 'sources.db'
FILES_PATH = './books'

TRANSLATION_TABLE = str.maketrans({
        # '.':'',
        # '?':'',
        ' ' :'',
        ',' :'',
        '"' :'',
        ':' :'',
        ';' :'',
        "'" :'',
        '-' :' ',
        '\n':' ',
        '\t':' ',
    })

def filter_str(s):
    """Filters a the input string to prepare it for the database."""
    s = s.lower()
    return s.translate(TRANSLATION_TABLE)


def fetch_pair(filename):
    """Returns a generator that fetches the next keypair from a text file"""
    with open(filename, "r") as fin:
        source = fin.read()

    words = source.split(" ")
    words = [filter_str(w) for w in words if (filter_str(w) is not "") or (filter_str(w) is not " ")]

    # Some of the source objects are large so free them as soon as possible.
    del(source)

    for i in range(0, len(words) - 2):
        key = words[i] + " " + words[i + 1]
        word = words[i + 2]

        if key == "" or key == " " or word == "" or word == " ":
            continue
        
        yield key, word


# TODO move this to the main application
def create_db_if_not_exists(db_location):
    if not os.path.isfile(db_location):
        conn = sqlite3.connect(db_location)
        c = conn.cursor()
        
        tables_sql = [\
'CREATE TABLE Sources (\
       Name TINYTEXT NOT NULL PRIMARY KEY\
);', \
'CREATE TABLE Tweets (\
       Message TINYTEXT NOT NULL,\
       TweetDate DEFAULT CURRENT_TIMESTAMP,\
       PRIMARY KEY (Message, TweetDate)\
);', \
'CREATE TABLE Words (\
       ID INTEGER PRIMARY KEY,\
       This TINYTEXT NOT NULL,\
       Next TINYTEXT NOT NULL\
);']
        
        for sql in tables_sql:
            c.execute(sql)
            
        conn.commit()
        c.close()
        conn.close()

        
def get_files(base_dir):
    return [f'{base_dir}/{x}' for x in os.listdir(base_dir) if os.path.isfile(f"{base_dir}/{x}")]


# TODO this doesnt seem to work... It always returns all of the files...
def new_sources(base_dir, db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    
    sql_result = c.execute("SELECT * FROM Sources")
    scanned_sources = []
    conn.commit()

    for row in c.fetchall():
        scanned_sources += row

    c.close()
    conn.close()
    return [x for x in get_files(base_dir) if x not in scanned_sources]
    

def main():
    create_db_if_not_exists(DB_NAME)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    added_values = False
    
    sql_query = 'INSERT INTO Words (This, Next) VALUES'

    for source in new_sources(FILES_PATH, DB_NAME):
        added_values = True
            
        for key, val in fetch_pair(source):
            sql_query += f'("{key}", "{val}"),'

    if added_values:
        c.execute(sql_query[:-1])
        conn.commit()
    
    c.close()
    conn.close()
    
main()
