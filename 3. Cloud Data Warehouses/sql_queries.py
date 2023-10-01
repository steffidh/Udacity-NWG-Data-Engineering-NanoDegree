import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    event_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    artist VARCHAR, 
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId BIGINT,
    song VARCHAR,
    status VARCHAR,
    ts VARCHAR,
    userAgent VARCHAR,
    userId INT  
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR NOT NULL PRIMARY KEY,
    artist_id VARCHAR NOT NULL,
    latitude FLOAT,
    longitude FLOAT,
    location VARCHAR,
    artist_name VARCHAR NOT NULL,
    duration FLOAT,
    num_songs INT,
    title VARCHAR,
    year INT
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY, 
    start_time TIMESTAMP, 
    user_id INT NOT NULL, 
    level VARCHAR, 
    song_id VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    session_id BIGINT, 
    location VARCHAR, 
    user_agent VARCHAR
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id INT NOT NULL PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR
)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INT,
    duration FLOAT
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL PRIMARY KEY, 
    name VARCHAR, 
    location VARCHAR, 
    lattitude FLOAT, 
    longitude FLOAT
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL, 
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL,
    month INT NOT NULL, 
    year INT NOT NULL, 
    Weekday INT NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("COPY staging_events FROM {} IAM_ROLE {} JSON {};").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))
    

staging_songs_copy = ("COPY staging_songs FROM {} IAM_ROLE {} JSON 'auto';").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))
                                   

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
        SELECT DISTINCT to_timestamp(to_char(events.ts,'9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
                        events.userId    AS user_id,
                        events.level     AS level,
                        songs.song_id    AS song_id,
                        songs.artist_id  AS artist_id,
                        events.sessionId AS session_id,
                        events.location  AS location,
                        events.userAgent AS user_agent
         FROM staging_events events
         JOIN staging_songs songs
          ON events.song = songs.title
          AND events.artist = songs.artist_name;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT userId    AS user_id,
                        firstName AS first_name,
                        lastName  As last_name,
                        gender,
                        level 
        FROM staging_events
        WHERE userId IS NOT NULL;      
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id,
                        title,
                        artist_id,
                        year,
                        duration 
        FROM staging_songs
        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
        SELECT DISTINCT artist_id,
                        artist_name AS name,
                        location,
                        latitude    AS lattitude,
                        longitude 
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, 
            extract(hour from start_time)      AS hour,
            extract(day from start_time)       AS day,
            extract(week from start_time)      AS week, 
            extract(month from start_time)     AS month,
            extract(year from start_time)      AS year, 
            extract(dayofweek from start_time) AS weekday
        FROM songplays
        WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
