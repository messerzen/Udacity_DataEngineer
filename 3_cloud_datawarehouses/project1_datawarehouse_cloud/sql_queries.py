import configparser
from typing import NamedTuple


# CONFIG
config = configparser.ConfigParser()
config.read('3_cloud_datawarehouses/project1_datawarehouse_cloud/dwh.cfg')
IAM=config['IAM_ROLE']['ARN']
LOG_DATA=config['S3']['LOG_DATA']
LOG_JSONPATH=config['S3']['LOG_JSONPATH']
SONG_DATA=config['S3']['SONG_DATA']


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist              VARCHAR,
    auth                VARCHAR,
    firstName           VARCHAR,
    gender              VARCHAR,
    itemInSession       INT,
    lastName            VARCHAR,
    length              DECIMAL,
    level               VARCHAR,
    location            VARCHAR,
    method              VARCHAR,
    page                VARCHAR,
    registration        BIGINT,
    sessionId           INT,
    song                VARCHAR,
    status              INT,
    ts                  BIGINT,
    userAgent           VARCHAR,
    userId              INT        
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INT,
    artist_id           VARCHAR,
    artist_latitude     DECIMAL,
    artist_longitude    DECIMAL,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR,
    title               VARCHAR,
    duration            DECIMAL,
    year                INT    
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
    songplay_id     BIGINT      IDENTITY(0,1),
    start_time      TIMESTAMP,
    user_id         INT,
    level           VARCHAR,
    song_id         VARCHAR,
    artist_id       VARCHAR,
    session_id      VARCHAR,
    location        VARCHAR,
    user_agent      VARCHAR
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
    user_id         INT             NOT NULL,
    first_name      VARCHAR,
    last_name       VARCHAR,
    gender          VARCHAR,
    level           VARCHAR
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
    song_id         VARCHAR         NOT NULL,
    title           VARCHAR,
    artist_id       VARCHAR,
    year            INT    ,
    duration        DECIMAL 
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
    artist_id       VARCHAR         NOT NULL,
    name            VARCHAR,
    location        VARCHAR,
    latitude        DECIMAL,
    longitude       DECIMAL
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
    start_time      TIMESTAMP,
    hour            INT,
    day             INT,
    week            INT,
    month           INT,
    year            INT,
    weekday         INT          
);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events 
                        FROM {}
                        iam_role {}
                        region 'us-west-2'
                        compupdate off
                        format as json {};
""").format(LOG_DATA, IAM, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs 
                        FROM {}
                        iam_role {}
                        region 'us-west-2'
                        compupdate off
                        json 'auto';
""").format(SONG_DATA, IAM)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, 
song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 Second' AS start_time,  
    e.userId                  AS      user_id,
    e.level                   AS      level,
    s.song_id                 AS      song_id,
    s.artist_id               AS      artist_id,
    e.sessionId               AS      session_id,
    e.location                AS      location,
    e.userAgent               AS      user_agent
FROM staging_events e
JOIN staging_songs s ON ( e.song = s.title 
                        AND e.length = s.duration
                        AND e.artist = s.artist_name)
WHERE 
    e.page = 'NextSong';       
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT 
    DISTINCT(userId)        AS      user_id,
    firstName               AS      first_name,
    lastName                AS      last_name,
    gender                  AS      gender,
    level                   AS      level
FROM staging_events
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT(song_id)       AS      song_id,
    title                   AS      title,
    artist_id               AS      artist_id,
    year                    AS      year,
    duration                AS      duration
FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artist(artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT(artist_id)     AS      artist_id,
    artist_name             AS      name,
    artist_location         AS      location,
    artist_latitude         AS      latitude,
    artist_longitude        AS      longitude
FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, 
week, month, year, weekday)
SELECT 
    a.start_time                        AS      start_time,
    EXTRACT(HOUR FROM a.start_time)     AS      hour,
    EXTRACT(DAY FROM a.start_time)      AS      day,
    EXTRACT(WEEK FROM a.start_time)     AS      week,
    EXTRACT(MONTH FROM a.start_time)    AS      month,
    EXTRACT(YEAR FROM a.start_time)     AS      year,
    EXTRACT(WEEKDAY FROM a.start_time)  AS      weekday
FROM 
    (SELECT TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 Second' AS start_time FROM staging_events e) a
WHERE
    a.start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
