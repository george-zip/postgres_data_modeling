import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
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



def process_log_file(cur, filepath):
    # open log file
    events_df = pd.read_json(filepath, lines=True)
    # filter for NextSong events
    next_song_df = events_df[events_df["page"] == "NextSong"]

    # convert timestamp to datetime
    t = pd.to_datetime(next_song_df['ts'], unit='ms')

    # insert time data records
    time_data = (next_song_df['ts'], t.dt.hour, t.dt.isocalendar().day, t.dt.isocalendar().week,
                 t.dt.month, t.dt.isocalendar().year, t.dt.day_of_week)
    column_labels = [
        "start_time", "hour", "day", "week", "month", "year", "weekday"
    ]
    time_df = pd.DataFrame(zip(*time_data), columns=column_labels)
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # insert users records
    valid_users = events_df["userId"] != ""
    user_df = events_df[valid_users][[
        "userId", "firstName", "lastName", "gender", "level"
    ]]
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for i, row in next_song_df.iterrows():

        # get song_id and artist_id from songs and artists tables
        if row.song:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        songplay_data = (row.ts, row.userId, row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
