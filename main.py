#!/usr/bin/env python3

from twython import Twython
from random import randint
import sqlite3
import time
import sys
import os

APP_KEY = ""
APP_SECRET = ""
OAUTH_TOKEN = ""
OAUTH_TOKEN_SECRET = ""

KEY_FILE = ".keys"

STATUS_UPDATE_DELAY = 86400 # Number of seconds in a day
DB_NAME = 'sources.db'

def get_start_words():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    sql = "SELECT This FROM Words ORDER BY RANDOM() LIMIT 1"
    c.execute(sql)
    conn.commit()

    (( res, ), ) = c.fetchall()

    return res

def stop_condition(words):
    if sum([len(x)+1 for x in words]) > 130:
        return True
    return False


def generate_message():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    words = get_start_words().split(" ")
    bits = {}

    while not stop_condition(words):
        last = f'{words[-2:][0]} {words[-1:][0]}'
        sql = f"SELECT Next FROM Words WHERE This = '{last}' ORDER BY RANDOM() LIMIT 1"

        c.execute(sql)
        conn.commit()

        ( ( new_word, ), ) = c.fetchall()

        words += [new_word]

    c.close()
    conn.close()

    message = ""

    for x in words:
        message += f"{x} "

    return message.capitalize()[:-1] + "\n - BOT"

def get_keys(kfile):
    with open(kfile) as fin:
        f = fin.read()

    f = f.split("\n")
    
    app_key = f[0].strip()
    app_secret = f[1].strip()
    oauth_token = f[2].strip()
    oauth_token_secret = f[3].strip()
    
    return app_key, app_secret, oauth_token, oauth_token_secret

def main():

    app_key, app_secret, oauth_token, oauth_token_secret = get_keys(KEY_FILE)

    while True:
        message = generate_message()
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        sql = f"INSERT INTO Tweets (Message) VALUES ('{message}')"

        c.execute(sql)
        conn.commit()

        c.close()
        conn.close()

        print(message)

        twitter = Twython(app_key, app_secret, oauth_token, oauth_token_secret)
        twitter.update_status(status=message)

        time.sleep(STATUS_UPDATE_DELAY)
        
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nShutting down ⬇')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
