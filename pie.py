import snowflake.connector as sc
from config import snow_flake_config
from lib.Logger import Logger
from lib.Variables import Variables

conn = sc.connect(user=snow_flake_config.username, password=snow_flake_config.password,
                  account=snow_flake_config.account)
variables = Variables("etc/ENV.cfg")
variables.set("SCRIPT_NAME", "te")
log = Logger(variables)


def execute_query(connection, __query):
    cursor = connection.cursor()
    cursor.execute(__query)
    cursor.close()


try:

except Exception as e:
    print(e)
