import pandas as pd
import cx_Oracle as orcl

result_columns = ['SITE', 'CLIENT_VERSION', 'VERSION_DATE', 'DB_HOST_NAME', 'ORACLE_VERSION','ORACLE_EDITION', 'INSTANCE_STATUS', 'RESTRICTED_LOGIN', 'DB_STATUS','DB_FREE_SPACE(GB)']


def parameter_file_load(x):
    try:
        with open(x) as json_file:
            text_input = pd.read_json(json_file)
            return text_input
    except IOError:
        print("File Handling Error: ", IOError)


def oracle_connect(db_ip, db_user, db_pass, db_sid):
    try:
        dsn_tns = orcl.makedsn(host=db_ip, port=1521, service_name=db_sid)
        return orcl.connect(user=db_user, password=db_pass, dsn=dsn_tns)
    except orcl.DatabaseError:
        raise


def read_query(x):
    open_file = open(x)
    full_sql = open_file.read()
    sql_commands = full_sql.split(';')
    return sql_commands


def multi_query(sql, curr, site):
    result_list = []
    for i in sql:
        run = curr.execute(i)
        for result in run:
            list_conv = list(result)
            result_list += list_conv
    result_list.insert(0, site)
    return result_list
