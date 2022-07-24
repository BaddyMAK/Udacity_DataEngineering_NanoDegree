import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime


def process_song_file(cur, filepath):
    """
    The process_song_file function takes as input the path for json file where the song and artist details exists,
    Then selects the required fields/ columns for each table (song or artist) and insert one record in one of each two tables
    for example : for the song table one record contains the next fields: song_id, title, artist_id, year, duration
    Arguments:
            cur: the cursor object.
            filepath: log song data file path.

        Returns:
            None
    """
    # open song file
    df = pd.read_json(filepath, typ='dictionary')

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        The process_log_file reads log files and select rows based on page=='NextSong' as mentioned on the instructions.
        Then converts the timestamp from unix format into human readable format (start_time),
        The converted datetime is split into hour, day, weed, month, year and weekday
        It select the fields or columns for the user table also that are the next: userId, firstName, lastName, gender, level
        The last point done by this function is inserting songplay records after selecting the songid, artistid using sql query           implemented in sql_queries.py
        If this query does not return nothing so the songid and artistid are assigned to NONE each. 
    
    Arguments:
            cur: the cursor object.
            filepath: log data file path.

        Returns:
            None
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']

    # convert timestamp column to datetime
    t = pd.DataFrame([datetime.datetime.fromtimestamp(s/1000.0).strftime('%Y-%m-%d %H:%M:%S') for s in df['ts']])
    t=pd.to_datetime(t[0])
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday ]
    column_labels = [ 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        timestamp=datetime.datetime.fromtimestamp(row.ts/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        songplay_data = [timestamp, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ 
        This function process_data get all the json files from a mentioned directory
        and concatenate them into a list then writes each processed file. 
        To gives you an idea how much files were successfly processed when the program is been executed.
        
        Arguments:
            cur: the cursor object.
            conn: connection to the database.
            filepath: log data or song data file path.
            func: function that transforms the data and inserts it into the database.

        Returns:
            None
        
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()