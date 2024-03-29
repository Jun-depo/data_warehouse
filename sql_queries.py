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

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
artist text, 
auth text, 
firstname text, 
gender text, 
iteminsession int, 
lastname text, 
length real, 
level text, 
location text, 
method text, 
page text, 
registration real, 
sessionid int, 
song text, 
status int, 
ts bigint, 
useragent text,  
user_id int)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
num_songs int, 
artist_id text, 
artist_latitude real, 
artist_longitude real, 
artist_location text, 
artist_name text,
song_id text, 
title text, 
duration real, 
year int)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplay_id int IDENTITY(0,1) PRIMARY KEY distkey, 
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level varchar(5), 
song_id varchar(50), 
artist_id varchar(50), 
session_id int NOT NULL, 
location varchar(80), 
user_agent varchar)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY sortkey, 
firstname varchar(30), 
lastname varchar(30), 
gender varchar(4), 
level varchar(5))
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
song_id varchar(50) PRIMARY KEY, 
title varchar, 
artist_id varchar(50) NOT NULL, 
year int sortkey, 
duration real)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY, 
name varchar, 
location varchar, 
latitude real, 
longitude real)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events from {}
IAM_ROLE {}
region 'us-west-2'
json {};
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""COPY staging_songs from {}
IAM_ROLE {}
region 'us-west-2'
FORMAT as JSON 'auto';
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time, se.user_id, se.level, ss.song_id, ss.artist_id, se.sessionid, se.location, se.useragent 
FROM staging_events se
JOIN staging_songs ss ON se.song=ss.title and se.artist=ss.artist_name and se.length = ss.duration
WHERE se.page = 'NextSong' AND se.user_id IS NOT NULL
""")

user_table_insert = ("""INSERT INTO users (user_id, firstname, lastname, gender, level)
SELECT DISTINCT user_id, firstname, lastname, gender, level FROM staging_events
WHERE user_id IS NOT NULL AND page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week,  month, year, weekday)
SELECT DISTINCT start_time, EXTRACT(hour From start_time), EXTRACT(day From start_time), EXTRACT(week From start_time), EXTRACT(month From start_time), EXTRACT(year From start_time), EXTRACT(DOW From start_time)
FROM songplays
""" )

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
