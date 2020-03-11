import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes all of the queries in the drop_table_queries list defined inside of sql_queries.py.
    Executing this will drop all Redshift tables.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes all of the queries in the create_table_queries list defined inside of sql_queries.py
    Executing this will create all Redshift tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main method for controlling the flow of dropping existing Redshift tables and creating/recreating tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("Successful connection to Redshift")
    cur = conn.cursor()

    print("Dropping existing tables...")
    drop_tables(cur, conn)
    print("Creating tables...")
    create_tables(cur, conn)
    print("Table reset complete.")
    conn.close()


if __name__ == "__main__":
    main()