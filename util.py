import pandas as pd
import snowflake.connector
from mysql import connector as mc
from mysql.connector import errorcode as ec
from snowflake import connector as sc
from const import load_operation
import psycopg2

from config.config import DB_DETAILS


def load_db_details(env):
    return DB_DETAILS[env]


def get_snowflake_connection(db_user, db_pass, db_acc, db_warehouse, db_database, db_schema):
    try:
        connection = snowflake.connector.connect(user=db_user,
                                                 password=db_pass,
                                                 account=db_acc,
                                                 warehouse=db_warehouse,
                                                 database=db_database,
                                                 schema=db_schema)
        cursor = connection.cursor()
        cursor.execute("select current date;")
        print(cursor.fetchone()[0])
    except sc.Error as error:
        print(error)
    return connection


def get_mysql_connection(db_type, db_host, db_name, db_user, db_pass):
    try:
        connection = mc.connect(user=db_user,
                                password=db_pass,
                                host=db_host,
                                database=db_name
                                )
    except mc.Error as error:
        if error.errno == ec.ER_ACCESS_DENIED_ERROR:
            print("Invalid Credentials")
        else:
            print(error)
    return connection


def get_tables(path, table_list):
    tables = pd.read_csv(path, sep=':')
    if table_list == 'all':
        return tables.query(load_operation)
    else:
        table_list_df = pd.DataFrame(table_list.split(','), columns=['table_name'])
        return tables.join(table_list_df.set_index('table_name'), on='table_name', how='inner'). \
            query(load_operation)


def get_tables(path):
    tables = pd.read_csv(path, sep=':')
    return tables.query(load_operation)


def get_pg_connection(db_host, db_name, db_user, db_pass):
    return psycopg2.connect(f"dbname={db_name} user={db_user} host={db_host} password={db_pass}")


def get_tables_from_text(path):
    tables = pd.read_csv(path, sep=':')
    return tables.query('to_be_loaded === "yes"')
