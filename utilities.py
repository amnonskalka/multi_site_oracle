import pandas as pd
import cx_Oracle as orcl

result_columns = ['SITE', 'CLIENT_VERSION', 'VERSION_DATE', 'DB_HOST_NAME', 'ORACLE_VERSION','ORACLE_EDITION', 'INSTANCE_STATUS', 'RESTRICTED_LOGIN', 'DB_STATUS', 'DB_FREE_SPACE_GB', 'DB_USED_SPACE_GB']


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


def collect_data(sql, con, site):
    '''the func is getting 3 parameters : sql - list of query's, con - connection string to connect the oracle, site - client site name
    the func run each query and return all of the results in single dataframe type
    '''
    temp_df = pd.DataFrame(columns=result_columns)
    for query in sql:
        query_result = pd.read_sql(query, con=con)
        df = pd.DataFrame(query_result)
        for name in list(df):
            temp_df[name] = df[name]
    temp_df['SITE'] = site.upper()
    return temp_df


