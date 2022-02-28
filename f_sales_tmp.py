from lib.Variables import Variables
from lib.Logger import Logger
import snowflake.connector as sc
from config import snow_flake_config
from snowflake.connector import DictCursor

conn = sc.connect(user=snow_flake_config.username, password=snow_flake_config.password,
                  account=snow_flake_config.account)
variables = Variables("etc/ENV.cfg")
variables.set("SCRIPT_NAME", "f_sales_tmp")
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
    execute_query(conn, "truncate bhatbhateni_tmp.sales")

    query = """
            INSERT INTO bhatbhateni_tmp.sales
            (id,store_id,product_id,customer_id,transaction_time,quantity,amount,discount)
            select id,store_id,product_id,customer_id,transaction_time,quantity,amount,discount from 
            bhatbhateni_stg.sales
    """
    execute_query(conn, query)

    log.log_message("insert to temp table")

except Exception as e:
    print(e)

finally:
    conn.close()
    log.close()
