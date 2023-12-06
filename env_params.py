# ========================================================================
# Variable values ==> change these values everytime scipt is executed
# Foramt YYYY-MM-DD <<== input to query edw
min_query_date = "2019-01-01"
start_date = "2019-01-01"
end_date = "2019-01-31"
batch_size = '1 week'


# refered in merge_proms_demographics
elhs_id_max = 21574
redcap_existing_data_file_name = 'redcap_existing_data_jan_june.csv'

# ========================================================================
# Static values ==> update only when necessary
# refered in format_medications
medication_code_book_file_name = 'medication_code_book.csv'
# refered in format_smart_form_smart_phrase
ilae_classification_mapping_file_name = 'ilae_classification_mapping.xlsx'
# not actively used
edw_redcap_variables_file_name = 'edw_redcap_variables_join_v2.xlsx'

# input to store edw and redcap data
preprocess_folder = "PHShome/jyr47/data_folder"

# input to refer static / reference data
inputs_folder = "PHShome/jyr47/inputs_folder"

# input to query edw
input_date_format = "%Y-%m-%d"

# input to add dates to file name
file_date_format = "%Y%b%d"

# ========================================================================
# DB Connection details ==> update only when necessary

# database credentails to connect to edw
mgb_user='jyr47'
mgb_pswd ='Hifiwifi@01'
edw_db_url = f"mssql+pyodbc://PARTNERS\\{mgb_user}:{mgb_pswd}@phsedw.partners.org:1433/Epic?driver=FreeTDS"
elhs_db_url = f"mssql+pyodbc://PARTNERS\\{mgb_user}:{mgb_pswd}@PHSSQL2026.partners.org:1433/elhsdcc?driver=FreeTDS"

import pandas as pd
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)




