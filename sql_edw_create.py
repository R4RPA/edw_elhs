from typing import Literal
import sqlite3
import pandas as pd
import os
import glob


def create_db(db_name: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_name)
    return conn


def push_excel_to_sqlite(db_conn: sqlite3.Connection,
                         folder_path: str,
                         if_exists: Literal['fail', 'replace', 'append'] = 'replace'):

    for excel_file in glob.glob(os.path.join(folder_path, '*.xlsx')):
        table_name = os.path.splitext(os.path.basename(excel_file))[0]
        print(f'{table_name} - start')
        df = pd.read_excel(excel_file)
        df.to_sql(table_name, db_conn, index=False, if_exists=if_exists)
        print(f'{table_name} - end')


# Example usage
db_conn = create_db('db/elhs.db')
#push_excel_to_sqlite(db_conn, 'data', if_exists='append')
db_conn.close()
