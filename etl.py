import psycopg2

from config import CONFIG
from sql_queries import COPY_TABLE_QUERIES, INSERT_TABLE_QUERIES


def load_staging_tables(cursor, connection):
    for query in COPY_TABLE_QUERIES:
        cursor.execute(query)
        connection.commit()


def insert_tables(cursor, connection):
    for query in INSERT_TABLE_QUERIES:
        cursor.execute(query)
        connection.commit()


def main():
    config_values = CONFIG['DB'].values()
    connection_string = "host={} dbname={} user={} password={} port={}"

    connection = psycopg2.connect(connection_string.format(*config_values))

    cursor = connection.cursor()
    
    load_staging_tables(cursor, connection)
    insert_tables(cursor, connection)

    connection.close()


if __name__ == "__main__":
    main()
