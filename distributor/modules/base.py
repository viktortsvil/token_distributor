from pprint import pprint
import random

from notion_database.database import Database

from distributor.modules.db import fetch_all_students
from distributor.modules.snapshot import generate_new_snapshot, retrieve_old_snapshot, compare_snapshots, \
    save_new_snapshot
from distributor.config import NOTION_KEY, STUDENT_DB_ID
from distributor.modules.koos import grant_tokens


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
        print(a, p, a < p)
        if a < p:
            result += 1
    return result


def main():
    database = _get_database(NOTION_KEY)
    all_students = fetch_all_students(database, STUDENT_DB_ID)
    df_new = generate_new_snapshot(all_students)
    df_old = retrieve_old_snapshot()
    comparison = compare_snapshots(df_old, df_new)
    pprint(comparison)
    save_new_snapshot(df_new)
    for key, value in comparison.items():
        n_tokens = binom(value['students_new'], 0.5)
        print(value['students_new'], key, n_tokens)
        if n_tokens:
            grant_tokens(key[0], key[1], n_tokens, 'Hola You Get Some Tokens')
            pass
