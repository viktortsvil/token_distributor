import logging
from collections import defaultdict as ddict

from notion_database.database import Database

from distributor.config import NOTION_KEY, STUDENT_DB_ID, TEAM_DB_ID, LOCATIONS_DB_ID
from distributor.modules.distributions import binom
from distributor.modules.db import fetch_all_students
from distributor.modules.koos import grant_tokens
from distributor.modules.snapshot import generate_student_db_snapshot, retrieve_old_snapshot, compare_snapshots, \
    save_new_snapshot

from pprint import pprint


def new_students():
    database = _get_database(NOTION_KEY)
    all_students = fetch_all_students(database, STUDENT_DB_ID)
    df_new = generate_student_db_snapshot(all_students)
    retrieved, df_old = retrieve_old_snapshot()
    comparison = compare_snapshots(df_old, df_new)
    logging.debug(comparison)
    save_new_snapshot(df_new)
    if retrieved:  # not having retrieved an old snapshot represents an old run
        for key, value in comparison.items():
            n_tokens = binom(value['students_new'], 0.25)
            logging.info(f"{n_tokens} generated out of {value['students_new']} for {key[1]}")
            if n_tokens:
                try:
                    grant_tokens(key[0], key[1], n_tokens, 'Hey! Good job on signing an agreement with a client!')
                    logging.info(f"Granted {n_tokens} to {key[1]} successfully")
                except BaseException as err:
                    logging.info(f"Failed granting tokens to {key[1]}. Error: {str(err)}")
    else:
        logging.info("Old Snapshot not Found. New Snapshot Saved. Exiting...")


def active_students():
    """
    This pipeline is to be run monthly, and thus it does not update the student .csv snapshot automatically. Rather,
    it relies on the snapshot update from pp.new_students.

    TODO: Move snapshot update into a separate pipeline.

    :return: None
    """

    # process the snapshot of active students and convert it into a dict by locations
    retrieved, df_old = retrieve_old_snapshot()
    if not retrieved:
        logging.info("No old snapshot was found. Exiting.")
        return
    town_counts_series = df_old.groupby('location')['location'].count()
    town_counts = ddict(int)
    town_counts = town_counts_series.to_dict(town_counts)

    # fetch all team members and validate their fields, then parse
    database = _get_database(NOTION_KEY)
    all_team_members = fetch_all_students(database, TEAM_DB_ID)
    for employee in all_team_members:
        employee_location = employee['properties']['Location']['relation']
        employee_name = employee['properties']['Name']['title']
        employee_position = [pos['name'] for pos in employee['properties']['Position']['multi_select']]
        employee_email = employee['properties']['Email']['email']
        if 'Team Lead' in employee_position:
            employee_position = 'Team Lead'
        else:
            employee_position = 'Not Team Lead'
        if not len(employee_name) or employee_email is None:
            continue
        employee_name = employee_name[0]['plain_text']
        employee_locations_id = [loc['id'] for loc in employee_location]

        employee_tokens = 0
        employee_divider = 10 if employee_position == 'Team Lead' else 20
        for loc_id in employee_locations_id:
            employee_tokens += town_counts[loc_id] / employee_divider
        employee_tokens = round(employee_tokens)
        if employee_tokens:
            try:
                grant_tokens(employee_name, employee_email, employee_tokens, 'Hey! Good job maintaining students in the locations!')
                logging.info(f"Granted {employee_tokens} to {employee_name} successfully")
            except BaseException as err:
                logging.info(f"Failed granting tokens to {employee_name}. Error: {str(err)}")

def _get_database(NOTION_KEY):
    return Database(NOTION_KEY)
