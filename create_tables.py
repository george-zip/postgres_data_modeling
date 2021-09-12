import psycopg2
from sql_queries import create_table_queries, drop_table_queries, log_data_staging_import_table_drop
from config_mgr import ConfigMgr
from argparse import ArgumentParser


def get_configuration_mgr():
    """
    Retrieve path for configuration file and use it to construct a ConfigMgr
    Note: This may not be the place best to share this function. I may move somewhere else.
    """
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", help="Path of yaml config file", default='./config.yaml')
    cmd_line = parser.parse_args()
    return ConfigMgr(cmd_line.config)


def create_database(cfg):
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

    # retrieve database credentials
    # connect to default database
    conn = psycopg2.connect(
        f"host={cfg.get('postgres_host')} dbname={cfg.get('landing_dbname')} user={cfg.get('user')} "
        f"password={cfg.get('password')}"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def drop_staging_table(cur, conn):
    """
    Drops staging table for log data
    """
    cur.execute(log_data_staging_import_table_drop)
    conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database(get_configuration_mgr())
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()