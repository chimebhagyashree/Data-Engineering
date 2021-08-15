import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Extract data from AWS S3 buckets
    - Load into staging tables using COPY command
    - arguments : cur, conn 
                - python cursor object to connect to database and execute queries
                - conn provides the connection to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - Inserts query statements to inserts data from staging table into target tables
    - arguments : cur, conn 
                - python cursor object to connect to database and execute queries
                - conn provides the connection to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - To call config files for credential authorization and database connection 
    - To call load_staging_tables and insert_tables fucntions to load into stage and target tables in redshift cluster database.
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()