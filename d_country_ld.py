import snowflake.connector as sc
from snowflake.connector import DictCursor

from config import snow_flake_config
from lib.Logger import Logger
from lib.Variables import Variables

conn = sc.connect(user=snow_flake_config.username, password=snow_flake_config.password,
                  account=snow_flake_config.account)
variables = Variables("etc/ENV.cfg")
variables.set("SCRIPT_NAME", "d_country_ld")
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

    query = """
            INSERT INTO bhatbhateni_tgt.country_dim
            (country_id,country_desc,record_active)
            select id,country_desc,1 from 
            bhatbhateni_tmp.country
    """
    execute_query(conn, query)

    log.log_message("insert to target table")

except Exception as e:
    print(e)

finally:
    conn.close()
    log.close()
