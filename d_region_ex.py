import snowflake.connector as sc
from snowflake.connector import DictCursor
from config import snow_flake_config
import util
import pandas as pd
from lib.Logger import Logger
from lib.Variables import Variables

conn = sc.connect(user=snow_flake_config.username, password=snow_flake_config.password,
                  account=snow_flake_config.account)
variables = Variables("etc/ENV.cfg")
variables.set("SCRIPT_NAME", "d_region_ex")
log = Logger(variables)


def execute_query(connection, __query):
    cursor = connection.cursor()
    cursor.execute(__query)
    cursor.close()


def fetch_from_source(__query):
    conn = util.get_mysql_connection('localhost', 'bhatbhateni', 'root', 'root')
    result = pd.read_sql_query(__query, conn)
    log.log_message("fetch from source")
    log.log_message("compressed source result to file")
    return result


def compressed_to_csv(result, file_name):
    result.to_csv(r".\csv\sales\{0}".format(file_name),
                  index=False,
                  header=False)


try:
    query = 'use {}'.format("BHATBHATENI")
    execute_query(conn, query)

    query = 'use warehouse {}'.format("COMPUTE_WH")
    execute_query(conn, query)

    try:
        query = 'alter warehouse {} resume'.format("COMPUTE_WH")
        execute_query(conn, query)
    except Exception as e:
        print(e)
    compressed_to_csv(fetch_from_source("select * from region"), variables.get("SCRIPT_NAME"))
    csv_file = r".\csv\sales\region"
    query = 'put file://{0} @{1} auto_compress=true'.format(csv_file, "BHAT_BHATENI_STAGE")
    log.log_message("uploaded compressed file to snoflake stage")
    execute_query(conn, query)

    log.log_message("truncate stage table")
    execute_query(conn, "truncate bhatbhateni_stg.region")

    query = "copy into {0} from @{1}/region.gz FILE_FORMAT=(TYPE=csv field_delimiter='," \
            "' skip_header=0)" \
            "ON_ERROR ='ABORT_STATEMENT'".format("bhatbhateni_stg.region", "BHAT_BHATENI_STAGE")
    execute_query(conn, query)

except Exception as e:
    print(e)
finally:
    conn.close()
    log.close()
