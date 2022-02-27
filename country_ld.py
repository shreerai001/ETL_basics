from lib.Variables import Variables
from lib.Logger import Logger
import snowflake.connector as sc
from config import snow_flake_config
from snowflake.connector import DictCursor


conn = sc.connect(user=snow_flake_config.username, password=snow_flake_config.password,
                  account=snow_flake_config.account)
variables = Variables("etc/ENV.cfg")
variables.set("SCRIPT_NAME", "country_ld")
log = Logger(variables)


def execute_query(connection, __query):
    cursor = connection.cursor()
    cursor.execute(__query)
    cursor.close()


try:
    query = 'use {}'.format("BHATBHATENI")
    execute_query(conn, query)

    query = 'use warehouse {}'.format("COMPUTE_WH")
    execute_query(conn, query)

    log.log_message("truncate temp table")
    execute_query(conn, "truncate bhatbhateni_tmp.country")

    query = """
            INSERT INTO bhatbhateni_tmp.country
            (country_id,country_desc)
            select country_id,country_desc from 
            bhatbhateni_stg.country
    """
    execute_query(conn, query)

    log.log_message("insert to temp table")

    query = 'select * from {}'.format("bhatbhateni_tmp.country")
    cursor = conn.cursor(DictCursor)
    cursor.execute(query)
    for c in cursor:
        print(c)
    cursor.close()
except Exception as e:
    print(e)

finally:
    conn.close()
    log.close()
