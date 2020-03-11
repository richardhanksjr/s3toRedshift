import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes all of the queries in the copy_table_queries list defined inside of sql_queries.py.
    This is where we actually execute the load from S3 to Redshift.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes all of the queries in the insert_table_queries list defined inside of sql_queries.py.
    This is where we load from the Redshift staging tables to the final schema.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main method for controlling the flow of loading data from S3 to Redshift and then the load to the final tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Loading json files from S3 into Redshift...")
    load_staging_tables(cur, conn)
    print("Initial table load from S3 to Redshift staging tables is complete.")
    print("Loading final tables from staging tables...")
    insert_tables(cur, conn)
    print("ETL complete.")

    conn.close()


if __name__ == "__main__":
    main()