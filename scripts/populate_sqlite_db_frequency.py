#!/usr/bin/python
#-*- coding: utf-8 -*-


import pathlib
import sqlite3


folder_project_root = pathlib.Path(__name__).absolute().parents[1]


# Source
filename_data_frequency = "internet-ru-num.txt"
folder_data_raw = folder_project_root / "data" / "raw"
filepath_data_input_frequency = folder_data_raw / filename_data_frequency


# Target
filename_db = "ru.db"
folder_data_processed = folder_project_root / "data" / "processed"
filepath_db = folder_data_processed / filename_db


sql_create_table_frequency = """
CREATE TABLE IF NOT EXISTS frequency (
    rank integer PRIMARY KEY,
    count integer,
    token text
);
"""


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def tables_in_sqlite_db(conn):
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [
        v[0] for v in cursor.fetchall()
        if v[0] != "sqlite_sequence"
    ]
    cursor.close()
    return tables


def create_frequency_record(conn, frequency_record):
    """
    Insert a new frequency_record into the frequency table
    :param conn:
    :param frequency_record:
    :return: frequency
    """
    sql = ''' INSERT INTO frequency(rank,count,token)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, frequency_record)
    conn.commit()
    return cur.lastrowid


def process_line_in_freq_text(line):
    line_split = line.split(" ")
    assert len(line_split) == 3
    # Use .rstrip() to remove newline characters '\n'
    frequency_record = (
        int(line_split[0]),
        int(line_split[1]),
        line_split[2].rstrip(),
    )
    return frequency_record


def print_progress(i, n_total, ndigits=2):
    msg = f"Completed {i} of {n_total} ({(i / n_total):.{ndigits}%})"
    print(msg, end='\r', flush=True)


def main():

    n_lines_skipped = 4

    number_of_lines = len(open(filepath_data_input_frequency).readlines())
    number_of_lines -= n_lines_skipped

    conn = sqlite3.connect(filepath_db)

    # create_table(conn, sql_create_table_frequency)

    print(tables_in_sqlite_db(conn))
    
    with open(filepath_data_input_frequency, 'r') as file:
        for i, line in enumerate(file):
            # Skip first 4 lines
            if i < n_lines_skipped:
                continue

            frequency_record = process_line_in_freq_text(line)
            create_frequency_record(conn, frequency_record)

            print_progress(i, number_of_lines)



if __name__ == "__main__":
    # execute only if run as a script
    main()
