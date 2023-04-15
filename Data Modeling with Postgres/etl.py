import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """

    # open song file
    df = pd.read_json(filepath, lines= True)

    # insert song record
    song_data =df[['song_id', 'title', 'artist_id', 'year','duration']].values.tolist()[0]
    #convert year column datatype from numpy.int64 to int to overcome error while running etl.py
    song_data[3] = int(song_data[3])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    df.rename(columns={"artist_name": "name", "artist_location": "location", "artist_latitude": "latitude"
                  , "artist_longitude": "longitude"} , inplace = True)
    artist_data = df[['artist_id', 'name', 'location', 'latitude', 'longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
     """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the user information in order to store it into the users table.
    Then it extracts the time information in order to store it into the time table.
    finally, it it extracts the songplays information in order to store it into the songplays table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] =  pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (df.ts, df.ts.dt.hour, df.ts.dt.day, df.ts.dt.week, df.ts.dt.month, df.ts.dt.year,  df.ts.dt.weekday_name)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') 
    time_df = pd.DataFrame( columns= column_labels)
    time_df['start_time'] = time_data[0]
    time_df['hour'] = time_data[1]
    time_df['day'] = time_data[2]
    time_df['week'] = time_data[3]
    time_df['month'] = time_data[4]
    time_df['year'] = time_data[5]
    time_df['weekday'] = time_data[6] 

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent) 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # function to iterate through files in filepath and process them
    """
    This procedure processes iterate through files in filepath and process them 

    INPUTS: 
    * cur the cursor variable
    * conn of the database 
    * filepath the file path to the log or song file
    * func which indicates processing of song/log data files
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
    the main function used to get data from data files and process them into previous functions to fill the tables 
    with corresponding data
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()