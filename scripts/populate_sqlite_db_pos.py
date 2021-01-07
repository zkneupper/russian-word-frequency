#!/usr/bin/python
#-*- coding: utf-8 -*-


import pathlib
import sqlite3


folder_project_root = pathlib.Path(__name__).absolute().parents[1]


# Source
filename_data_pos = "dictionary.txt"
folder_data_raw = folder_project_root / "data" / "raw"
filepath_data_input_pos = folder_data_raw / filename_data_pos


# Target
filename_db = "ru.db"
folder_data_processed = folder_project_root / "data" / "processed"
filepath_db = folder_data_processed / filename_db


sql_create_table_part_of_speech = """
CREATE TABLE IF NOT EXISTS part_of_speech (
    group_number integer,
    word text,
    pos text,
    pos_specific text
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


def create_pos_record(conn, pos_record):
    """
    Insert a new pos_record into the part_of_speech table
    :param conn:
    :param pos_record:
    :return: ?
    """
    sql = ''' INSERT INTO part_of_speech(group_number,word,pos,pos_specific)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, pos_record)
    conn.commit()
    return cur.lastrowid


def print_progress(i, n_total, ndigits=2):
    msg = f"Completed {i} of {n_total} ({(i / n_total):.{ndigits}%})"
    print(msg, end='\r', flush=True)


def main():

    number_of_lines = len(open(filepath_data_input_pos).readlines())

    conn = sqlite3.connect(filepath_db)

    create_table(conn, sql_create_table_part_of_speech)

    print(tables_in_sqlite_db(conn))
    
    with open("../data/raw/dictionary.txt", "r") as file:
        for i, line in enumerate(file):
            line = line.strip()
            
            if line.isdigit():
                group_number = int(line)
            
            elif (line == ""):
                continue
            
            else:
                text = line.split("\t")
                assert len(text) == 2
                word = text[0]
                pos_list = text[1].split(",", maxsplit=1)
                pos = pos_list[0]
                
                if len(pos_list) == 2:
                    pos_specific = pos_list[1]
                else:
                    pos_specific = ""
                
                # group_number, Word, POS, POS specific
                pos_record = (
                    group_number,
                    word,
                    pos,
                    pos_specific
                )

                create_pos_record(conn, pos_record)
            
            print_progress(i, number_of_lines)


if __name__ == "__main__":
    # execute only if run as a script
    main()
