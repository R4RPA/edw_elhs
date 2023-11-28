from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import env_params as env  # Importing environment variables for connection strings
import pandas as pd


class EdwDb:
    def __init__(self):
        db_url = env.edw_db_url  # Using MS SQL Server connection string from environment
        try:
            self.engine = create_engine(db_url, echo=False)
            print("Connection to EDW database established successfully.")
        except SQLAlchemyError as e:
            print(f"Error connecting to the EDW database: {e}")

    def query(self, sql, dataframe=False):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(sql)
                data = result.fetchall()  # Fetch all data
                if dataframe:
                    columns = list(result.keys())  # Get column names
                    return pd.DataFrame(data, columns=columns)
                else:
                    return data
        except SQLAlchemyError as e:
            print(f"EdwDb: Error executing query: {e}")
            return None

    def execute(self, sql):
        try:
            with self.engine.connect() as conn:
                conn.execute(sql)
                conn.commit()  # Commit the transaction
                return True
        except SQLAlchemyError as e:
            print(f"EdwDb: Error executing query: {e}")
            return False

    def read_sql(self, sql):
        try:
            return pd.read_sql(sql, self.engine)
        except SQLAlchemyError as e:
            print(f"EdwDb: Error executing query: {e}")
            return None


class ElhsDb:
    def __init__(self):
        db_url = env.elhs_db_url  # Using MS SQL Server connection string from environment
        try:
            self.engine = create_engine(db_url, echo=False)
            print("Connection to ELHS database established successfully.")
        except SQLAlchemyError as e:
            print(f"Error connecting to the ELHS database: {e}")

    def query(self, sql, dataframe=False):
        try:
            with self.engine.connect() as conn:
                result = conn.execute(sql)
                data = result.fetchall()  # Fetch all data
                if dataframe:
                    columns = list(result.keys())  # Get column names
                    return pd.DataFrame(data, columns=columns)
                else:
                    return data
        except SQLAlchemyError as e:
            print(f"ElhsDb: Error executing query: {e}")
            return None

    def execute(self, sql):
        try:
            with self.engine.connect() as conn:
                conn.execute(sql)
                conn.commit()  # Commit the transaction
                return True
        except SQLAlchemyError as e:
            print(f"ElhsDb: Error executing query: {e}")
            return False

    def read_sql(self, sql):
        try:
            return pd.read_sql(sql, self.engine)
        except SQLAlchemyError as e:
            print(f"ElhsDb: Error executing query: {e}")
            return None
