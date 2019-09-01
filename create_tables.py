import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """A function for Dropping existing tables with matching names in the queries. So, these tables can be created in clean states."""   
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """A function for creating tables based on create_table_queries."""  
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Execute create_tables.py to create tables at AWS Redshift""" 
    
    #Get config info of AWS redshift database.
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to AWS redshift database with the config info.\ 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Execute drop_tables() and cteate_tables() functions
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # close the connection when the operations are done
    conn.close()


if __name__ == "__main__":
    main()