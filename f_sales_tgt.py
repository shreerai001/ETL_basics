import snowflake.connector as sc

from config import snow_flake_config
from lib.Logger import Logger
from lib.Variables import Variables

conn = sc.connect(user=snow_flake_config.username, password=snow_flake_config.password,
                  account=snow_flake_config.account)
variables = Variables("etc/ENV.cfg")
variables.set("SCRIPT_NAME", "sales_tgt")
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

    query = """           insert into bhatbhateni_tgt.month_dim
            (month_record) select month(transaction_time) from bhatbhateni_tmp.sales"""
    execute_query(conn, query)
    log.log_message("successfully loaded dimension")

    query = """
            INSERT INTO bhatbhateni_tgt.F_BHATBHATENI_AGG_SLS_PLC_MONTH_T
            (id,store_id,product_id,customer_id,quantity,amount,discount,record_active,month_id)
            select id,store_id,product_id,customer_id,quantity,amount,discount,1,m.month_record
             from bhatbhateni_tmp.sales s
            inner join bhatbhateni_tgt.month_dim m on m.month_record = month(s.transaction_time)
    """

    log.log_message("successfully loaded fact")

    execute_query(conn, query)

    log.log_message("insert to target table")

except Exception as e:
    print(e)

finally:
    conn.close()
    log.close()
