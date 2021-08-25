import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all specified existing tables in redshift cluster before create the new ones.

    Args:
        cur (object): cursor for interact with aws redshift cluster.
        conn (object): connection with amazon redshift cluster.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates the specified tables in amazon redshift cluster.
    Args:
        cur (object): cursor for interact with aws redshift cluster.
        conn (object): connection with amazon redshift cluster.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Creates a config parser object.
    - Parses the dhw.cgf file (amazon aws connection informations).
    - Creates an amazon redshift connnection object using psycopg2 library.
    - Creates a cursor object to interact with amazon redshift cluster.
    - Drop the specified existing tables.
    - Creates the new specified tables.
    """
    config = configparser.ConfigParser()
    config.read('3_cloud_datawarehouses/project1_datawarehouse_cloud/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()