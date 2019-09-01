import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """A function for loading data into staging_tables, that copies data from S3 buckets into staging_tables."""  
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """A function for insert data into tables."""  
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Execute the program to load data into staging tables and insert data into dimension and fact tables. """ 
    
    # Get AWS Redshift database config info.
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to Redshift database with config info.  
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Execute load_staging_tables() and insert_tables() to load data into the tables 
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # close the connection when the operations are done.  
    conn.close()


if __name__ == "__main__":
    main()