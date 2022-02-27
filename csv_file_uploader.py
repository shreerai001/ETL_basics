import snowflake.connector as sc
from snowflake.connector import DictCursor
from config import snow_flake_config
import util
import pandas as pd


def execute_query_snowflake(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


try:
    conn = sc.connect(user=snow_flake_config.username, password=snow_flake_config.password)
    query = 'use warehouse {}'.format(snow_flake_config.database)
    execute_query_snowflake(conn, query)

    query = 'alter warehouse {} resume'.format(snow_flake_config.warehouse)
    execute_query_snowflake(conn, query)

    csv_file = r"C:\Users\shrikrishna.rai\PycharmProjects\ETL_basics\csv\sales\data-00000"
    query = 'put file://{0} @{1} auto_compress=true'.format(csv_file, snow_flake_config.stage_table)
    execute_query_snowflake(conn, query)

except:
    pass
