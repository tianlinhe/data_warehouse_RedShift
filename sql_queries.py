import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender CHAR(1),
itemInSession INT,
lastName VARCHAR,
length NUMERIC,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration NUMERIC,
sessionId INT,
song VARCHAR,
status INT,
ts BIGINT,
userAgent VARCHAR,
userId INT);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(num_songs INT,
artist_id VARCHAR,
artist_latitude NUMERIC,
artist_longitude NUMERIC,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration NUMERIC,
year INT);
""")

# Notice: dtype of "start_time" is converted from BIGINT (in staging_events) to TIMESTAMP, 
# for 1) convenience
# 2) creation of "time" table (it contains day, hour etc, using TIMSTAMP is easier)
# SERIAL is not supported, hence we use IDENTITY(0,1)
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays                                               
(songplay_id INT IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP,
user_id VARCHAR,
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id VARCHAR,
location VARCHAR,
user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(user_id VARCHAR PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender CHAR(1),
level VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR,
year INT,
duration NUMERIC);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude NUMERIC,
longitude NUMERIC);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time TIMESTAMP PRIMARY KEY,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT);
""")

# STAGING TABLES
# copy from S3 bucket to staging

# 1. staging events
# - why we need to define log_json_path.json?
# for matching between key-columns when copying a table from MULTIPLE paths
# for test with smaller dataset, use config.get('S3_sub','LOG_DATA')

staging_events_copy = ("""COPY staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2' compupdate off
JSON 's3://udacity-dend/log_json_path.json'
""").format(config.get('S3', 'LOG_DATA'),config.get('IAM_ROLE', 'ARN'))

# 2. staging songs
# - Add TRUNCATECOLUMNS to COPY statement: automatically truncate any varchar # field that exceeds the size limit. 
# Goal: improve success rate
# - JSON 'auto': default of copying from json files
# for test with smaller dataset, use config.get('S3_sub','SONG_DATA')

staging_songs_copy = ("""COPY staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2' compupdate off
JSON 'auto' truncatecolumns
""").format(config.get('S3', 'SONG_DATA'),config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

# Remarks: LEFT JOIN is necessary to keep all records in "staging_events"
# set three joining criteria:
# 1) song name can match
# 2) artist name can match
# 3) song length can match
# set one filtering criterion: page in "staing_events" must be 'NextSong'
# Remarks: convert ts from BIGINT to TIMESTAMP, is not necessary, but it will make sence for
# 1) information
# 2) easy creation of "time" table

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' , e.userId, e.level, s.song_id, s.artist_id,
e.sessionId, e.location, e.userAgent
FROM staging_events AS e
LEFT JOIN staging_songs AS s
ON (e.song=s.title) AND (e.artist=s.artist_name) AND (e.length=s.duration) AND (e.page='NextSong') AND (e.ts IS NOT NULL)
""")

# SELECT DISTINCT is necessary because staging_events table is based on the log of songs ever played,
# so one user can appear many times and he has always the same firstname, lastname etc
# => SELECT DISTINCT to keep unique user!

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userID, firstName, lastName, gender, level
FROM staging_events
WHERE userID IS NOT NULL
""")

# SELECT DISTINCT is a no, because staging_songs is a database of songs,
# scenario 1: one song should probably appear only once => SELECT DISTINCT is equivalent to SELECT
# scenario 2: actually one song can be sung by different singers
# => one song can appear more than once => Using SELECT DISTINCT will unfavourably remove those songs

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

# SELECT DISTINCT is necessary, because staging_songs is a database of songs
# => one artist can appear more than once if he sang > 1 songs
# => SELECT DISTINCT keeps the unique artists
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

# Extract info from songplay table
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time, DATE_PART(hr, start_time), DATE_PART(d, start_time), DATE_PART(w, start_time),
        DATE_PART(mon, start_time), DATE_PART(y, start_time), DATE_PART(dow,start_time)
FROM songplays
WHERE start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
