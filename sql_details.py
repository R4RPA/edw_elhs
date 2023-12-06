import sqlite3

def list_tables_and_columns(db_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    # Create a cursor object using the cursor() method
    cursor = conn.cursor()

    # Retrieve all tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Iterate over each table and retrieve column details
    for table in tables:
        table_name = table[0]
        print(f"Table: {table_name}")
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        for column in columns:
            column_name = column[1]
            data_type = column[2]
            print(f"  Column: {column_name}, Type: {data_type}")
        print("\n")

    # Close the connection
    conn.close()

# Example usage
db_path = 'db/edw.db'
list_tables_and_columns(db_path)


