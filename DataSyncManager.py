import os
import re
import json
import datetime
from DatabaseConnections import EdwDb, ElhsDb
from dateutil.relativedelta import relativedelta
import pandas as pd
import env_params as env


class DataSyncManager:
    def __init__(self, track_folder):
        self.track_folder = track_folder
        self.user_inputs = env
        self.destination_tables = ['smartform']
        self.query_path = "queries"
        self.edw_db = EdwDb()
        self.elhs_db = ElhsDb()

    def get_max_date(self, site, folder, table_name, db):
        print(f'get dates from {folder} - {table_name}')
        # Construct the file path for the SQL query
        query_path = os.path.join(self.query_path, f'{site}_{folder}', f'edw_date_{table_name}.sql')
    
        # Check if the file exists
        if os.path.exists(query_path):
            # Read the SQL query from the file
            with open(query_path, 'r') as file:
                sql = file.read()
            # Execute the query and return the result
            result = db.query(sql.replace('\n',''))
            max_date = pd.to_datetime(result[0][0]).date() if result else None
            print(max_date)
            return max_date

        else:
            return None

    def get_valid_fetch_range(self, site, table_name):
        #src_max_date = self.get_max_date(site, 'SRC', table_name, self.edw_db)
        dest_max_date = self.get_max_date(site, 'DEST',table_name, self.elhs_db)
        #dest_max_date = pd.to_datetime('2023-03-20')
        src_max_date = pd.to_datetime('2023-12-20')

        user_fetch_from = self.user_inputs.start_date.date()
        user_fetch_to = self.user_inputs.end_date.date()

        if not dest_max_date:
            fetch_from = user_fetch_from
            # Ensure fetch_from date is not after src_max_date
            fetch_to = min(user_fetch_to, src_max_date)
            # If fetch_from is after fetch_to, no valid range is available
            if fetch_from > fetch_to:
                return None, None
            return fetch_from, fetch_to

        # Start fetching from the later of (the day after dest_max_date) and (user's fetch_from),
        # but within the source data availability
        fetch_from = user_fetch_from
        print('dest_max_date', dest_max_date, type(dest_max_date))
        print('pd.Timedelta(days=1)', pd.Timedelta(days=1), type(pd.Timedelta(days=1)))
        if dest_max_date and dest_max_date + pd.Timedelta(days=1) > fetch_from:
            next_day_after_dest = dest_max_date + pd.Timedelta(days=1)
            if next_day_after_dest <= src_max_date:
                fetch_from = next_day_after_dest

        # Ensure fetch_from date is the day after dest_max_date
        fetch_from = min(fetch_from, dest_max_date + pd.Timedelta(days=1))

        # Determine the latest date to fetch to, which is the earlier of src_max_date or user's fetch_to
        fetch_to = min(src_max_date, user_fetch_to)

        # If fetch_from is after fetch_to, no valid range is available
        if fetch_from > fetch_to:
            return None, None

        return fetch_from, fetch_to

    def process_table(self, site, table_name):
        fetch_from, fetch_to = self.get_valid_fetch_range(site, table_name)
        print('process_table', fetch_from, fetch_to)
        if not fetch_from:
            return False

        batch_size = self.user_inputs.batch_size.lower()
        json_file_name = os.path.join(self.track_folder, site, f"{site}_{table_name}_{fetch_from}_{fetch_to}.json").replace(' 00:00:00', '')
        os.makedirs(os.path.join(self.track_folder, site), exist_ok=True)
        if not os.path.exists(json_file_name):
            self.create_json_file(json_file_name, fetch_from, fetch_to, batch_size)
        return True

    def create_json_file(self, file_name, start_date, end_date, batch_size):
        print('create_json_file')
        batches = list(self.calculate_batches(start_date, end_date, batch_size))
        data = {os.path.basename(file_name).split('_')[1]: {
            f"{b[0]}_{b[1]}".replace(' 00:00:00', ''): {'source_fetch_status': 'OPEN', 'destination_push_status': 'OPEN'} for b in batches}}
        with open(file_name, 'w') as file:
            json.dump(data, file, default=str, indent=4)

    def calculate_batches(self, start_date, end_date, batch_size):
        current = start_date
        while current <= end_date:
            batch_end_date = self.calculate_end_date(current.strftime('%Y-%m-%d'), batch_size)
            # Ensure that batch_end_date does not exceed the overall end_date
            batch_end_date = min(batch_end_date.date(), end_date)
            yield current, batch_end_date
            # Set the next start date to be the day after the current batch_end_date
            current = batch_end_date + datetime.timedelta(days=1)

    def calculate_end_date(self, start_date_str, batch_size_str):
        # Convert start_date string to datetime object
        start_date = pd.to_datetime(start_date_str)

        # Extract number and unit from batch_size_str
        match = re.match(r"(\d+) (\w+)", batch_size_str)
        if not match:
            raise ValueError("Invalid batch size format")

        number = int(match.group(1))
        unit = match.group(2)

        # Calculate end date based on the unit
        if unit in ["day", "days"]:
            end_date = start_date + relativedelta(days=number)
        elif unit in ["week", "weeks"]:
            end_date = start_date + relativedelta(weeks=number)
        elif unit in ["month", "months"]:
            end_date = start_date + relativedelta(months=number)
        elif unit in ["year", "years"]:
            end_date = start_date + relativedelta(years=number)
        else:
            raise ValueError("Invalid time unit")

        # Adjust for inclusive counting (same start and end date for '1 day')
        if batch_size_str in ["1 day", "1 week", "1 month", "1 year"]:
            end_date -= relativedelta(days=1)

        return end_date

    def check_tables_to_process(self, sites):
        tables_to_process = False
        for site in sites:
            for table in self.destination_tables:
                if self.process_table(site, table):
                    tables_to_process = True
        return tables_to_process

def main():
    track_folder = os.path.join('sync_track', datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
    # track_folder = 'sync_track/20231121171644'
    data_sync_manager = DataSyncManager(track_folder)
    tables_to_process = data_sync_manager.check_tables_to_process(sites=['MGH'])
    print(f'are there tables to process: {"YES" if tables_to_process else "NO"}')

