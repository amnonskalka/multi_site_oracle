import pandas as pd
import cx_Oracle as orcl
import os
import logging


logging.basicConfig(filename='action_log.log', level=logging.DEBUG, filemode='w',
                    format='%(asctime)s - %(levelname)s: %(message)s')

result_columns = ['SITE', 'CLIENT_VERSION', 'VERSION_DATE',
                  'DB_HOST_NAME', 'ORACLE_VERSION','ORACLE_EDITION',
                  'INSTANCE_STATUS', 'RESTRICTED_LOGIN', 'DB_STATUS',
                  'DB_FREE_SPACE_GB', 'DB_USED_SPACE_GB']


def parameter_file_load(x):
    if os.path.exists(x):
        try:
            with open(x) as json_file:
                text_input = pd.read_json(json_file)
                return text_input
        except Exception as e:
            logging.error("There was issue to Open\Read the file Due to: " + str(e))
    else:
        logging.error("Could not find the query file in " + os.path.dirname(x))


def oracle_connect(db_ip, db_user, db_pass, db_sid):
    try:
        dsn_tns = orcl.makedsn(host=db_ip, port=1521, service_name=db_sid)
        return orcl.connect(user=db_user, password=db_pass, dsn=dsn_tns)
    except Exception as e:
        logging.error("There was issue with DB connection Due to: " + str(e))
        pass


def read_query(x):
    if os.path.exists(x):
        try:
            with open(x) as open_file:
                full_sql = open_file.read()
                sql_commands = full_sql.split(';')
                return sql_commands
        except Exception as e:
            logging.error("There was issue to Open\Read the file Due to: " + str(e))
            pass
    else:
        logging.error("Could not find the query file in " + os.path.dirname(x))


def collect_old(sql, con, site):
    '''
    The func' is getting 3 parameters :
    sql - list of query's,
    con - connection string to connect the oracle,
    site - client site name
    The func' run each query and return all of the results in single DataFrame type
    '''
    temp_df = pd.DataFrame(columns=result_columns)
    for query in sql:
        try:
            query_result = pd.read_sql(query, con=con)
            df = pd.DataFrame(query_result)
            for name in list(df):
                temp_df[name] = df[name]
        except Exception as e:
            print("Could not run the query due to " + str(e))
    temp_df['SITE'] = site.upper()
    return temp_df


def collect_data(sql, con, site):
    '''
    The func' is getting 3 parameters :
    sql - list of query's,
    con - connection string to connect the oracle,
    site - client site name
    The func' run each query and return all of the results in single DataFrame type
    '''
    if con is not None:
        temp_df = pd.DataFrame(columns=result_columns)
        for query in sql:
            try:
                query_result = pd.read_sql(query, con=con)
                df = pd.DataFrame(query_result)
                for name in list(df):
                    temp_df[name] = df[name]
            except Exception as e:
                logging.error("Could not run the query due to " + str(e))
        temp_df['SITE'] = site.upper()
        return temp_df
    else:
        logging.error("Connection parameter is empty, unable to run queries for " + site)
        pass
