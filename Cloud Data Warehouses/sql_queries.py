import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users "
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events  (artist varchar ,\
                                                                            auth varchar ,\
                                                                            firstName varchar ,\
                                                                            gender varchar ,\
                                                                            itemInSession int ,\
                                                                            lastName varchar ,\
                                                                            length Float ,\
                                                                            level varchar,\
                                                                            location varchar,\
                                                                            method varchar,\
                                                                            page varchar,\
                                                                            registration varchar,\
                                                                            sessionID int,\
                                                                            song varchar,\
                                                                            status int,\
                                                                            ts BIGINT,\
                                                                            userAgent varchar,\
                                                                            user_Id int);")







staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs    (num_songs int ,\
                                                                            artist_id varchar ,\
                                                                            artist_latitude Float ,\
                                                                            artist_longitude Float ,\
                                                                            artist_location varchar ,\
                                                                            artist_name varchar ,\
                                                                            song_id varchar ,\
                                                                            title varchar,\
                                                                            duration Float,\
                                                                            year int);")



songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays    ( songplay_id INT IDENTITY(0,1) PRIMARY KEY,\
                                                                    start_time TIMESTAMP NOT NULL sortkey,\
                                                                    user_id int  NOT NULL ,\
                                                                    level varchar ,\
                                                                    song_id varchar ,\
                                                                    artist_id varchar ,\
                                                                    session_id int ,\
                                                                    location varchar,\
                                                                    user_agent varchar);")




user_table_create = ("CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY,\
                                                        first_name varchar NOT NULL ,\
                                                        last_name varchar ,\
                                                        gender varchar ,\
                                                        level varchar );")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY,\
                                                        title varchar NOT NULL ,\
                                                        artist_id varchar ,\
                                                        year int ,\
                                                        duration NUMERIC NOT NULL );"
)

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY,\
                                                            name varchar NOT NULL ,\
                                                            location varchar,\
                                                            latitude Float,\
                                                            longitude Float);")

time_table_create = ("CREATE TABLE IF NOT EXISTS time ( start_time timestamp PRIMARY KEY sortkey,\
                                                        hour int ,\
                                                        day int ,\
                                                        week int ,\
                                                        month int ,\
                                                        year int ,\
                                                        weekday int );")

# STAGING TABLES
                       
staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}' 
compupdate off REGION 'us-west-2' 
FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])
                    

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}' 
REGION 'us-west-2' 
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])
                       
                    
                      
                      
# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays ( start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)

SELECT DISTINCT
    TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second') AS start_time,
    se.user_Id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent    
FROM staging_events se
LEFT JOIN staging_songs ss

ON se.song = ss.title
AND se.artist = ss.artist_name

WHERE se.page = 'NextSong';

""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT (user_Id),
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT (song_id),
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT (artist_id),
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
        SELECT DISTINCT ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
