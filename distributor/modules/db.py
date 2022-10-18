def fetch_all_students(database_obj, student_db_id):
    all_results = []
    cursor = None
    while not all_results or database_obj.result['has_more'] is True:
        print("New Patch...")
        database_obj.find_all_page(student_db_id, start_cursor=cursor)
        all_results.extend(database_obj.result['results'])
        cursor = database_obj.result['next_cursor']

    return all_results


