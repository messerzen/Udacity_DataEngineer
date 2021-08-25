import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data from s3 bucket files to amazon redshift staging tables.

    Args:
        cur (object): cursor for interact with aws redshift cluster.
        conn (object): connection with amazon redshift cluster.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inputs data from staging tables to the fact and dimensions table in amazon redshift cluster.

    Args:
        cur (object): cursor for interact with aws redshift cluster.
        conn (object): connection with amazon redshift cluster.
    """
    for query in insert_table_queries:
        print(f'Running query: \n {query}')
        cur.execute(query)
        conn.commit()


def main():
    """
    - Creates a config parser object.
    - Parses the dhw.cgf file (amazon aws connection informations).
    - Creates an amazon redshift connnection object using psycopg2 library.
    - Creates a cursor object to interact with amazon redshift cluster.
    - Loads data from s3 bucket files to amazon redshift staging tables.
    - Inserts data from staging tables to dimensions and fact tables.
    """
    config = configparser.ConfigParser()
    config.read('3_cloud_datawarehouses/project1_datawarehouse_cloud/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()