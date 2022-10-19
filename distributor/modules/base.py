import random
import logging
from datetime import datetime

from notion_database.database import Database

from distributor.modules.db import fetch_all_students
from distributor.modules.snapshot import generate_new_snapshot, retrieve_old_snapshot, compare_snapshots, \
    save_new_snapshot
from distributor.config import NOTION_KEY, STUDENT_DB_ID
from distributor.modules.koos import grant_tokens

logging.basicConfig(filename=f"distributor/log/{str(datetime.utcnow()).split('.')[0]}.txt", level=logging.DEBUG)


def _get_database(NOTION_KEY):
    return Database(NOTION_KEY)


def binom(n, p):
    """
    Returns binomial sample
    :param n: N
    :param p: Probability of success
    :return: K (int)
    """
    result = 0
    for i in range(n):
        a = random.random()
        if a < p:
            result += 1
    return result


def main():
    database = _get_database(NOTION_KEY)
    all_students = fetch_all_students(database, STUDENT_DB_ID)
    df_new = generate_new_snapshot(all_students)
    df_old = retrieve_old_snapshot()
    comparison = compare_snapshots(df_old, df_new)
    logging.debug(comparison)
    save_new_snapshot(df_new)
    for key, value in comparison.items():
        n_tokens = binom(value['students_new'], 0.5)
        logging.info(f"{n_tokens} generated out of {value['students_new']} for {key[1]}")
        if n_tokens:
            try:
                grant_tokens(key[0], key[1], n_tokens, 'Hey! Good job on signing an agreement with a client!')
                logging.info(f"Granted {n_tokens} to {key[1]} successfully")
            except BaseException as err:
                logging.info(f"Failed granting tokens to {key[1]}. Error: {str(err)}")
