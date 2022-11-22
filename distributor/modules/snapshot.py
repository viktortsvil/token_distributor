import logging

import pandas as pd

from distributor.config import SNAPSHOT_PATH


def generate_team_db_snapshot(members):
    raise NotImplementedError


def generate_student_db_snapshot(students):
    total = 0
    snapshot = []
    for student in students:
        communicator_list = student['properties']['Responsible Communicator']['people']
        status = student['properties']['Status']['select']
        personal_code = student['properties']['Personal Code']['number']
        city = student['properties']['Location']['relation']
        if len(city):
            city = city[0]['id']
        else:
            city = None
        total += 1
        if status and status['name'] == 'Active' and personal_code is not None:
            for communicator in communicator_list:
                if not (communicator.get('name') and communicator.get('person') and communicator['person'].get(
                        'email')):
                    continue
                snapshot.append({
                    'student_code': personal_code,
                    'com_name': communicator['name'],
                    'com_email': communicator['person']['email'],
                    'location': city
                })
    return pd.DataFrame(snapshot)


def retrieve_old_snapshot():
    try:
        df = pd.read_csv(SNAPSHOT_PATH)
        retrieved = True
    except:
        logging.info(f"No old snapshot was found at given path ({SNAPSHOT_PATH})")
        df = pd.DataFrame(columns=['student_code', 'com_name', 'com_email'])
        retrieved = False
    return retrieved, df


def save_new_snapshot(df_new: pd.DataFrame):
    df_new.to_csv(SNAPSHOT_PATH)


def compare_snapshots(df_old, df_new):
    old_coms = dict()
    new_coms = dict()

    groups_old = df_old.groupby(['com_name', 'com_email'])
    for com_name, group in groups_old:
        old_coms[com_name] = set(list(group.student_code.unique()))
        new_coms.setdefault(com_name, set([]))

    groups_new = df_new.groupby(['com_name', 'com_email'])
    for com_name, group in groups_new:
        new_coms[com_name] = set(list(group.student_code.unique()))
        old_coms.setdefault(com_name, set([]))

    stats = dict()
    for key, new_set in new_coms.items():
        old_set = old_coms[key]
        stats[key] = {
            'students_stayed': len(new_set.intersection(old_set)),
            'students_new': len(new_set - old_set),
            'students_left': len(old_set - new_set)
        }
    return stats
