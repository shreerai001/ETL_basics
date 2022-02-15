import psycopg2

server = 'localhost'
port = '5432'
user = 'postgres'
password = 'postgres'
database = 'bhat_bhateni'

if __name__ == '__main__':
    query = "select * from bhat_bhateni_sales.sales"
    try:
        connection = psycopg2.connect(host=server, port=port, user=user, password=password, database=database)
        cursor = connection.cursor()
        cursor.execute(query)

        count = 0
        while True:
            count += 1
            print(count)
            rows = cursor.fetchmany(1)

            if rows:
                print(rows)
            else:
                break

    except Exception as e:
        print(e)
