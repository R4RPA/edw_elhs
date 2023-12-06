from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import env_params as env
import pandas as pd


class EdwDb:
    def __init__(self):
        self.db_url = env.edw_db_url
        
    def connect(self):
        try:
            self.engine = create_engine(self.db_url, echo=False)
            print("Connection to EDW database established successfully.")
        except SQLAlchemyError as e:
            print(f"Error connecting to the EDW database: {e}")
        
    def query(self, sql, dataframe=False):
        try:
            self.connect()
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                data = result.fetchall()
                if dataframe:
                    columns = list(result.keys())
                    return pd.DataFrame(data, columns=columns)
                else:
                    return data
        except SQLAlchemyError as e:
            print(f"EdwDb: Error executing query: {e}")
            return None

    def execute(self, sql):
        try:
            self.connect()
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()  # Commit the transaction
                return True
        except SQLAlchemyError as e:
            print(f"EdwDb: Error executing query: {e}")
            return False

    def read_sql(self, sql):
        try:
            self.connect()
            return pd.read_sql(text(sql), self.engine)
        except SQLAlchemyError as e:
            print(f"EdwDb: Error executing query: {e}")
            return None


class ElhsDb:
    def __init__(self):
        self.db_url = env.elhs_db_url
    
    def connect(self):
        try:
            self.engine = create_engine(self.db_url, echo=False)
            print("Connection to ELHS database established successfully.")
        except SQLAlchemyError as e:
            print(f"Error connecting to the ELHS database: {e}")

    def query(self, sql, dataframe=False):
        try:
            self.connect()
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
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
            self.connect()
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()  # Commit the transaction
                return True
        except SQLAlchemyError as e:
            print(f"ElhsDb: Error executing query: {e}")
            return False

    def read_sql(self, sql):
        try:
            self.connect()
            return pd.read_sql(text(sql), self.engine)
        except SQLAlchemyError as e:
            print(f"ElhsDb: Error executing query: {e}")
            return None
    
    def push_to_sql(self, df, table, if_exists = 'append'):
        try:
            self.connect()
            df.to_sql(table, self.engine, index=False, if_exists=if_exists)
        except SQLAlchemyError as e:
            print(f"ElhsDb: Error push to sql: {e}")
            return None
    