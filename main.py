import utilities as ut
import pandas as pd

ut.logging.info('Process started')
pd.set_option('display.max_columns', 20)
param_path = r'd:\sites_json.json'
parameters_json = ut.parameter_file_load(param_path)
sql_path = r'd:\db_query.sql'
results = pd.DataFrame(columns=ut.result_columns)


for index, row in parameters_json.iterrows():
    site = row['site']
    db_ip = row['db_ip']
    db_user = row['user']
    db_pass = row['password']
    db_sid = row['sid']

    ut.logging.info('Start Working on ' + site.upper())

    try:
        conn = ut.oracle_connect(db_ip, db_user, db_pass, db_sid)
        sql = ut.read_query(sql_path)
        site_results = ut.collect_data(sql, conn, site)
        results = results.append(site_results)
        ut.logging.info('Finish working on ' + site.upper())
    except Exception as e:
        ut.logging.error(str(e) + 'was raise due to the running of the main script')
        continue

conn.close()

ut.logging.info('Process finished')
print(results)
