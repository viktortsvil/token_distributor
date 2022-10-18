from pprint import pprint

from notion_database.database import Database

from distributor.modules.db import fetch_all_students
from distributor.modules.snapshot import generate_new_snapshot, retrieve_old_snapshot, compare_snapshots, save_new_snapshot
from distributor.config import NOTION_KEY, STUDENT_DB_ID

def _get_database(NOTION_KEY):
    return Database(NOTION_KEY)


def main():
    database = _get_database(NOTION_KEY)
    all_students = fetch_all_students(database, STUDENT_DB_ID)
    df_new = generate_new_snapshot(all_students)
    df_old = retrieve_old_snapshot()
    comparison = compare_snapshots(df_old, df_new)
    pprint(comparison)
    save_new_snapshot(df_new)
