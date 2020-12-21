import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy
from psycopg2.extensions import register_adapter, AsIs

#dataframe columns
USER_COLUMNS = ['userId','firstName','lastName','gender','level']
SONG_COLUMNS = ['song_id','title','artist_id','year','duration']

ARTISTS_COLUMNS = ['artist_id','artist_name','artist_location',
                   'artist_latitude','artist_longitude']

#to register numpy types into inserts
def addapt_numpy(numpy_type):
    """Function to register numpy types in postgre database"""
    return AsIs(numpy_type)

def process_song_file(cur, filepath):
    """Function to load json song files into postgres database
        cur: postgres wrapper cursor
        filepath: file filepath
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[SONG_COLUMNS].iloc[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[ARTISTS_COLUMNS].fillna('').iloc[0]) 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Function to load json log files into postgres database
        cur: postgres wrapper cursor
        filepath: file filepath
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts']
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[USER_COLUMNS]

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, 
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        Get all files from the path passed.
        Iterate over json files, process and insert the data into the database.
        
        cur: pyscopg2 cursor object
        conn: psycopg2 connect object
        filepath: the path of json files
        func: the function used to process the data
    
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
    """
        Execute the ETL process.
        Create a connection with the database, retrieve a cursor.
        Apply the register_adapter to numpy types for correct data inserts database.
        Execute the process_data in two paths of song's data and log's data.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                            user=student password=student")
    cur = conn.cursor()
    
    #register numpy types into postgres
    register_adapter(numpy.float64, addapt_numpy)
    register_adapter(numpy.int64, addapt_numpy)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()