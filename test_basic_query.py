from DatabaseConnections import EdwDb, ElhsDb

# Initialize database connections
edw_db = EdwDb()
elhs_db = ElhsDb()

# Function to test basic queries
def test_basic_query(db, db_name):
    if db.engine:
        print(f"Testing basic query on {db_name} database.")

        # Example query: Select version or equivalent information
        # This query should be modified based on the actual database and its schema
        query = "SELECT @@VERSION;" if 'mssql' in db.engine.url.drivername else "SELECT sqlite_version();"

        # Test fetching data as a DataFrame
        df_result = db.query(query, dataframe=True)
        if df_result is not None:
            print(f"DataFrame result from {db_name} database:\n{df_result}")
        else:
            print(f"Failed to fetch DataFrame from {db_name} database.")

        # Test fetching data as raw data
        raw_result = db.query(query)
        if raw_result is not None:
            print(f"Raw data result from {db_name} database:\n{raw_result}")
        else:
            print(f"Failed to fetch raw data from {db_name} database.")
    else:
        print(f"Connection to {db_name} database failed.")

# Test basic queries on both databases
test_basic_query(edw_db, "EDW")
test_basic_query(elhs_db, "ELHS")
