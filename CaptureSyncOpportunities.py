import json
import os
import concurrent.futures
import pandas as pd
import env_params as env
import glob
from DatabaseConnections import EdwDb, ElhsDb
import threading

class CaptureSyncOpportunities:
    def __init__(self, track_folder):
        self.track_folder = track_folder
        self.user_inputs = env
        self.destination_tables = ['demographics', 'proms', 'codednotes']
        self.query_path = "queries"
        self.sites = ['MGH']
        self.edw_db = EdwDb()
        self.elhs_db = ElhsDb()
        self.lock = threading.Lock()

    def get_json_file_name(self, site, table):
        print('get_json_file_name')
        pattern = os.path.join(self.track_folder, site, f'{site}_{table}_*.json')
        files = glob.glob(pattern)
        return files[0] if files else None

    def load_json_files(self):
        print('load_json_files', 'start')
        opportunities = {'fetch_and_push': [], 'push_only': []}
        for site in self.sites:
            for table in self.destination_tables:
                json_file_name = self.get_json_file_name(site, table)
                if json_file_name and os.path.exists(json_file_name):
                    with open(json_file_name, 'r') as file:
                        data = json.load(file)
                        if table in data:
                            for batch_range, statuses in data[table].items():
                                if statuses['source_fetch_status'] == 'OPEN' and statuses['destination_push_status'] == 'OPEN':
                                    opportunities['fetch_and_push'].append((site, table, batch_range))
                                elif statuses['destination_push_status'] == 'OPEN':
                                    opportunities['push_only'].append((site, table, batch_range))
        print('load_json_files', 'end')
        return opportunities

    def start_data_operations(self):
        print('start_data_operations', 'start')
        opportunities = self.load_json_files()
        for op in opportunities['fetch_and_push']:
            self.fetch_and_push_data(*op)
        for op in opportunities['push_only']:
            print(op)
            self.push_data_to_elhs(*op)
        
        print('start_data_operations', 'end')

    def get_query(self, site, table, start_date, end_date):
        query_path = os.path.join(self.query_path, f'{site}_SRC', f'edw_data_{table}.sql')
        with open(query_path, 'r') as file:
            sql = file.read()
        return sql.format(dynamic_start_date=start_date, dynamic_end_date=end_date)

    def get_data_csv_path(self, site, table, batch_range):
        temp_csv_path = os.path.join(self.track_folder, site, 'data', f'{table}_{batch_range}.csv')
        os.makedirs(os.path.dirname(temp_csv_path), exist_ok=True)
        return temp_csv_path

    def update_json_file(self, site, table, batch_range, status_key, status_value):
        print('update_json_file', 'start')
        json_file_name = self.get_json_file_name(site, table)
        if json_file_name and os.path.exists(json_file_name):
            with self.lock:
                with open(json_file_name, 'r+') as file:
                    data = json.load(file)
                    if table in data and batch_range in data[table]:
                        data[table][batch_range][status_key] = status_value
                        file.seek(0)
                        json.dump(data, file, indent=4)
                        file.truncate()
        print('update_json_file', 'end')

    def fetch_and_push_data(self, site, table, batch_range):
        print(site, table, batch_range)
        print('fetch_and_push_data', 'start')
        query_path = os.path.join(self.query_path, f'{site}_SRC', f'edw_data_{table}.sql')
        batch_range_arr = batch_range.split('_')
        dynamic_start_date = batch_range_arr[0] + ' 00:00:00'
        dynamic_end_date = batch_range_arr[1] + ' 23:59:59'
        if os.path.exists(query_path):
            with open(query_path, 'r') as file:
                sql = file.read()
                sql = sql.format(dynamic_start_date=dynamic_start_date, dynamic_end_date=dynamic_end_date)
            df = self.edw_db.read_sql(sql)
            if not df.empty:

                # Save to CSV
                temp_csv_path = self.get_data_csv_path(site, table, batch_range)
                df.to_csv(temp_csv_path, index=False)
                # Update JSON file
                self.update_json_file(site, table, batch_range, 'source_fetch_status', 'COMPLETED')

                # Push data to ELHS - placeholder for actual implementation
                self.push_data_to_elhs(site, table, batch_range)
        print('fetch_and_push_data', 'end')

    def push_data_to_elhs(self, site, table, batch_range, if_exists = 'append'):
        print('push_data_to_elhs', 'start')
        temp_csv_path = self.get_data_csv_path(site, table, batch_range)
        df = pd.read_csv(temp_csv_path)
        self.elhs_db.push_to_sql(df, table, if_exists)

        # Update JSON file
        self.update_json_file(site, table, batch_range, 'destination_push_status', 'COMPLETED')
        print('push_data_to_elhs', 'end')

def main():
    track_folder = 'sync_track/20231129230124'
    data_sync_manager = CaptureSyncOpportunities(track_folder)
    data_sync_manager.start_data_operations()
