import snowflake.connector as sc
from snowflake.connector import DictCursor
from config import snow_flake_config
import util
import pandas as pd

conn = sc.connect(user=snow_flake_config.username,
                  password=snow_flake_config.password,
                  account=snow_flake_config.account)


def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor


try:
    query = 'use {}'.format("BHATBHATENI")
    execute_query(conn, query)

    query = 'use warehouse {}'.format("COMPUTE_WH")
    execute_query(conn, query)

    conn = util.get_mysql_connection('localhost', 'bhat_bhateni', 'root', 'root')
    cursor = conn.cursor()
    cursor.execute("select * from country")
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    df = df.transform()
    df.to_csv(r"C:\Users\shrikrishna.rai\PycharmProjects\ETL_basics\csv\sales\data-1111")
    try:
        query = 'alter warehouse {} resume'.format("COMPUTE_WH")
        execute_query(conn, query)
    except Exception as e:
        print(e)

    csv_file = r"C:\Users\shrikrishna.rai\PycharmProjects\ETL_basics\csv\sales\data-00000"
    query = 'put file://{0} @{1} auto_compress=true'.format(csv_file, "BHAT_BHATENI_STAGE")
    execute_query(conn, query)

    sql = "copy into {0} from @{1}/bhat_bhateni_stage/data-00000.gz FILE_FORMAT=(TYPE=csv field_delimiter='," \
          "' skip_header=0)" \
          "ON_ERROR ='ABORT_STATEMENT'".format("transactions.sales", "BHAT_BHATENI_STAGE")
    execute_query(conn, sql)

    sql = 'select * from {}'.format("transactions.sales")
    cursor = conn.cursor(DictCursor)
    cursor.execute(sql)
    for c in cursor:
        print(c)
    cursor.close()
except Exception as e:
    print(e)
finally:
    conn.close()
