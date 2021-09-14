import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from create_tables import drop_staging_table
from io import StringIO
from config_mgr import ConfigMgr
from create_tables import get_configuration_mgr


def process_song_file(cur, filepath):
    """
    Opens song file and create records in songs and artists table
    """

    # open song file
    # each file contains a single row
    df = pd.read_json(filepath, lines=True)

    # insert artist record first
    # there is a dependency in songs on artist_id
    artist_data = (df.iloc[0].artist_id, df.iloc[0].artist_name,
                   df.iloc[0].artist_location, df.iloc[0].artist_latitude,
                   df.iloc[0].artist_longitude)
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    # the default type for column year will be an int64 but psycopg2 expects a Python int type
    song_data = (df.iloc[0].song_id, df.iloc[0].title, df.iloc[0].artist_id,
                 int(df.iloc[0].year), df.iloc[0].duration)
    cur.execute(song_table_insert, song_data)


def load_staging_table(cur, filepath):
    """
    Copies content of JSON in filepath into a staging table for further processing
    """
    events_df = pd.read_json(filepath, lines=True)
    # buf will be a file-like object that we'll use in place of a csv file
    buf = StringIO()
    # load file contents into buf
    events_df.to_csv(buf, sep="|", header=False)
    # move starting position back to the start of the buffer
    buf.seek(0)
    # apply postgres copy function
    cur.copy_from(buf, 'log_data_staging', sep="|")


def populate_destination_tables(cur, conn):
    """
    Copies rows from staging to destination tables
    """
    cur.execute(time_table_insert_from_staging)
    print(f"{cur.rowcount} rows inserted into time")
    conn.commit()
    cur.execute(user_table_insert_from_staging)
    print(f"{cur.rowcount} rows inserted into user")
    conn.commit()
    cur.execute(songplays_table_insert_from_staging)
    print(f"{cur.rowcount} rows inserted into songplays")
    conn.commit()


def process_data(cur, conn, filepath, func):
    """
    Opens directory containing JSON files and applies the func parameter on each file
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Starting point for script
    Retrieves configuration and processes the JSON files
    """

    cfg = get_configuration_mgr()
    conn = psycopg2.connect(
        f"host={cfg.get('postgres_host')} dbname={cfg.get('sparkify_dbname')} user={cfg.get('user')} " 
        f"password={cfg.get('password')}"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath=cfg.get('song_data_location'), func=process_song_file)
    process_data(cur, conn, filepath=cfg.get('log_data_location'), func=load_staging_table)
    populate_destination_tables(cur, conn)
    # staging table not needed any more
    drop_staging_table(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
