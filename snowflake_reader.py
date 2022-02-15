import snowflake.connector as sf
from config import snow_flake_config
from snowflake.connector import DictCursor

conn = sf.connect(user=snow_flake_config.username,
                  password=snow_flake_config.password,
                  account=snow_flake_config.account)


def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


try:
    query = 'use {} '.format(snow_flake_config.database)
    execute_query(conn, query)

    query = 'use warehouse {}'.format(snow_flake_config.warehouse)
    execute_query(conn, query)

    query = 'alter warehouse {} resume'.format(snow_flake_config.warehouse)
    execute_query(conn, query)

    query = 'select * from TRANSACTIONS.SALES'
    cursor = conn.cursor(DictCursor)
    cursor.execute(query)
    for c in cursor:
        print(c)
    cursor.close()
except Exception as e:
    print(e)
finally:
    conn.close()
