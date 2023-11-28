from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os


class EdwDb:
    def __init__(self):
        db_url = f"sqlite:///{os.path.join('db', 'edw.db')}"
        try:
            self.engine = create_engine(db_url, echo=False)
            print(f"Connection to EDW database established successfully.")
        except SQLAlchemyError as e:
            print(f"Error connecting to the EDW database: {e}")

    def query(self, sql):
        try:
            with self.engine.connect() as conn:
                # Convert raw SQL string to a SQL text object
                sql_text = text(sql)
                return conn.execute(sql_text).fetchall()
        except SQLAlchemyError as e:
            print(f"EdwDb: Error executing query: {e}")
            return None


class ElhsDb:
    def __init__(self):
        db_url = f"sqlite:///{os.path.join('db', 'elhs.db')}"
        try:
            self.engine = create_engine(db_url, echo=False)
            print(f"Connection to ELHS database established successfully.")
        except SQLAlchemyError as e:
            print(f"Error connecting to the ELHS database: {e}")

    def query(self, sql):
        try:
            with self.engine.connect() as conn:
                # Convert raw SQL string to a SQL text object
                sql_text = text(sql)
                return conn.execute(sql_text).fetchall()
        except SQLAlchemyError as e:
            print(f"ElhsDb: Error executing query: {e}")
            return None
