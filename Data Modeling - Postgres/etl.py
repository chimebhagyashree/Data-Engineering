import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Description : 
    This function is designed for
        - processing all the song files(.json format) present in the directory
        - extracting attribute values for only song data from song files and inserting into the songs table
        - extracting attribute values for only artist data  from the song files and performing insert statements into artist table.
    
    Arguments :
        cur : cursor object
        filepath : song data file path
    
    Returns :
        None
        
    '''
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist() 
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Description : 
    This function is focusing on
         - processing all the log files(.json format) present in the directory
         - extracting data for the 'NextSong' page
         - converting timestamp attribute values to different time components for generating time data
         - inserting time data records into the time table.
         - extracting attribute values for only user data  from the log files and ingesting records into user table.
         - Getting the song_id and artist_id values from song and artist table for the song name,artist name and duration of the song from              the log file and other attributes from log file for songplays data.
         - Inserting the extracted data into songplays fact table
    
    Arguments :
        cur : cursor object
        filepath : log data file path
    
    Returns :
        None
    '''
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts,unit='ms')
    
    # insert time data records
    time_data = pd.concat([t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday],axis=1)
    column_labels = time_data.set_axis(['start_time','hour','day', 'week','month','year','weekday'],axis=1,inplace=True)
    time_df = time_data

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

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
        songplay_data = (pd.to_datetime(row.ts,unit='ms'),int (row.userId),row.level,row.sessionId,row.location,row.userAgent,songid,artistid)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Description: 
    This function is responsible for 
        - listing all the .json files in a directory
        - getting the total number of files present in that directory.
        - processing all files in an iterative manner according to the specified function
        - executing the data ingestion process to the database tables.
    

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    
    '''
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
    '''
         - Connecting to sparkify database
         - calling process_data() function to process song and log files for data ingestion process
         - closing the database connection
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()