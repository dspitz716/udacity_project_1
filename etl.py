import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import sklearn


def process_song_file(cur, filepath):
        """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    df = pd.read_json(filepath, lines=True)

    
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
        """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    df = pd.read_json(filepath, lines=True)

    
    df = df[df['page'] == 'NextSong']

    
    t = pd.to_datetime(df['ts'], unit='ms')
    
   
    time_data = ([x, x.hour, x.day, x.week, x.month, x.year, x.dayofweek] for x in t)
    column_labels = ('start_time','hour', 'day' ,'week', 'month','year','weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    
    user_df = df.filter(['userId','firstName','lastName','gender','level'])

   
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

   
    for index, row in df.iterrows():
        
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        
        songplay_data = ([row.ts, row.userId, row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure gets all files matching extension from the directory provided in the filepath variable. 
    It then gets the total number of files found and processes each one providing a display of the total files processed
    
    INPUTS: 
    conn the connection to db
    cur cursor variable
    filepath data filepath(s)
    func functions 
    
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
    
    '''definiton of main funtions in script'''
    
    
    
 
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
