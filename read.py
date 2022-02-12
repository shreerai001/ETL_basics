from util import get_mysql_connection
from loguru import logger


def get_column_names(cursor, db_type):
    return cursor.column_names


def read_table(db_details, table_name, limit=0):
    logger.info(db_details)
    connection = get_mysql_connection(db_type=db_details['DB_TYPE'],
                                      db_host=db_details['DB_HOST'],
                                      db_name=db_details['DB_NAME'],
                                      db_user=db_details['DB_USER'],
                                      db_pass=db_details['DB_PASS']
                                      )
    cursor = connection.cursor()
    if limit == 0:
        query = f'SELECT * FROM {table_name}'
    else:
        query = f'SELECT * FROM {table_name} LIMIT {limit}'
    cursor.execute(query)
    data = cursor.fetchall()
    column_names = get_column_names(cursor, db_details['DB_TYPE'])

    connection.close()

    return data, column_names
