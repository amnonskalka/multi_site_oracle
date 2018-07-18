import utilities as ut
import pandas as pd


param_path = r'd:\\sites_json.json'
parameters_json = ut.parameter_file_load(param_path)
sql_path = r'd:\\db_query.sql'
results = pd.DataFrame(columns=ut.result_columns)


for index, row in parameters_json.iterrows():
    site = row['site']
    db_ip = row['db_ip']
    db_user = row['user']
    db_pass = row['password']
    db_sid = row['sid']

    conn = ut.oracle_connect(db_ip, db_user, db_pass, db_sid)
    sql = ut.read_query(sql_path)
    curr = conn.cursor()
    site_results = ut.multi_query(sql, curr, site)

    print(site_results)


conn.close()
