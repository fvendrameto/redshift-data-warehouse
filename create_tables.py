import psycopg2

from config import CONFIG
from sql_queries import CREATE_TABLE_QUERIES, DROP_TABLE_QUERIES


def drop_tables(cursor, connection):
    for query in DROP_TABLE_QUERIES:
        cursor.execute(query)
        connection.commit()


def create_tables(cursor, conn):
    for query in CREATE_TABLE_QUERIES:
        cursor.execute(query)
        conn.commit()


def main():
    config_values = CONFIG['DB'].values()
    connection_string = "host={} dbname={} user={} password={} port={}"

    connection = psycopg2.connect(connection_string.format(*config_values))

    cursor = connection.cursor()

    drop_tables(cursor, connection)
    create_tables(cursor, connection)

    connection.close()


if __name__ == "__main__":
    main()
