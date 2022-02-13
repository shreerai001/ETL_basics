from loguru import logger

from read import read_table
import pandas as pd
from write import write_df_to_file, load_table
from util import get_tables, load_db_details, get_tables_from_text


def init_logger():
    logger.add("etl-demo.info",
               rotation="1 MB",
               retention="10 days",
               level="INFO"
               )
    logger.add("etl-demo.err",
               rotation="1 MB",
               retention="10 days",
               level="ERROR"
               )


def main():
    env = 'dev'
    a_database = 'RETAIL_DB'
    a_table = 'sales'
    a_tables = 'RETAIL_DB'
    init_logger()
    db_details = load_db_details(env)[a_database]
    logger.info(f'reading data for {a_table}')
    data, column_names = read_table(db_details, a_table)

    # write to csv file
    df = pd.DataFrame(data, columns=column_names)
    write_df_to_file(r'C:\Users\shrikrishna.rai\PycharmProjects\ETL_basics\tmp', table_name=a_table, df=df)

    # write to db
    db_details = load_db_details(env)
    tables = get_tables_from_text('table_list', a_tables)
    for table_name in tables['table_name']:
        logger.info(f'reading data for {table_name}')
        data, column_names = read_table(db_details, table_name)
        logger.info(f'loading data for {table_name}')
        load_table(db_details, data, column_names, table_name)


if __name__ == '__main__':
    main()
