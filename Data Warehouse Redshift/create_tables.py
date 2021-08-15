import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
     - DROP stage and target tables if already exists in the database
     - arguments : cur, conn 
                - python cursor object to connect to database and execute queries
                - conn provides the connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
     - CREATE stage and target tables if not exists in the database
     - arguments : cur, conn 
                - python cursor object to connect to database and execute queries
                - conn provides the connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - To call config files for credential authorization and database connection 
    - To call drop_tables and create_tables fucntions to drop and create tables in redshift cluster database.
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()