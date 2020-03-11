import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays_fact"
user_table_drop = "DROP TABLE IF EXISTS users_dim"
song_table_drop = "DROP TABLE IF EXISTS songs_dim"
artist_table_drop = "DROP TABLE IF EXISTS artists_dim"
time_table_drop = "DROP TABLE IF EXISTS time_dim"

# CREATE TABLES

staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS staging_songs
                                 (num_songs INT,
                                  artist_id VARCHAR(20),
                                  artist_latitude FLOAT,
                                  artist_longitude FLOAT,
                                  artist_name VARCHAR(255),
                                  song_id VARCHAR(20),
                                  title VARCHAR(255),
                                  duration FLOAT,
                                  year INT
                                 )
""")

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events
                                 (artist VARCHAR(255),
                                  auth VARCHAR(20),
                                  firstName VARCHAR(30),
                                  gender VARCHAR(1),
                                  itemInSession INT,
                                  lastName VARCHAR(30),
                                  length FLOAT,
                                  level VARCHAR(20),
                                  location VARCHAR(50),
                                  method VARCHAR(20),
                                  page VARCHAR(20),
                                  registration FLOAT,
                                  sessionId INT,
                                  song VARCHAR(255),
                                  status VARCHAR(20),
                                  ts BIGINT,
                                  userAgent VARCHAR(255),
                                  userId INT
                                 )
""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays_fact
                            (songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
                             start_time TIMESTAMP, 
                             user_id INT NOT NULL, 
                             level VARCHAR(20),
                             song_id VARCHAR(20) NOT NULL, 
                             artist_id VARCHAR(20) NOT NULL,
                             session_id INT NOT NULL, 
                             location VARCHAR(255),
                             user_agent VARCHAR(255)
                            )
""")

# Using a distribution style of all for all dimension tables due to the small size of each of these tables.
user_table_create = ("""CREATE TABLE IF NOT EXISTS users_dim
                        (user_id INT PRIMARY KEY,
                         first_name VARCHAR(20),
                         last_name VARCHAR(20),
                         gender VARCHAR(1),
                         level VARCHAR(20)
                        ) diststyle all
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs_dim
                        (song_id VARCHAR(20) PRIMARY KEY,
                         title VARCHAR(255) NOT NULL,
                         artist_id VARCHAR(20) NOT NULL,
                         year INT NOT NULL,
                         duration FLOAT NOT NULL
                        ) diststyle all
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists_dim
                          (artist_id VARCHAR(20) PRIMARY KEY,
                           name VARCHAR(255) NOT NULL,
                           location VARCHAR(50) NOT NULL,
                           latitude FLOAT,
                           longitude FLOAT                  
                          ) diststyle all
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_dim
                        (start_time DATETIME PRIMARY KEY,
                         hour INT NOT NULL,
                         day INT NOT NULL,
                         week INT NOT NULL,
                         month INT NOT NULL,
                         year INT NOT NULL,
                         weekday INT NOT NULL
                        ) diststyle all
""")

# STAGING TABLES

# We can use json 'auto' here because the column names map directly to the json file inputs
staging_songs_copy = ("""COPY staging_songs from {s3} credentials 'aws_iam_role={iam_role}'
json 'auto' compupdate off region 'us-west-2';
""").format(s3=config.get('S3', 'song_data'), iam_role=config.get('IAM_ROLE', 'arn'), json_file=config.get('S3', 'log_jsonpath'))


# We can NOT use json 'auto' here because the columns do not line up.  Instead, we are using the log_jsonpath to map our columns.
staging_events_copy = ("""COPY staging_events from {s3} credentials 'aws_iam_role={iam_role}'
FORMAT AS JSON {json_file} timeformat 'auto' compupdate off region 'us-west-2';
""").format(s3=config.get('S3', 'log_data'), iam_role=config.get('IAM_ROLE', 'arn'), json_file=config.get('S3', 'log_jsonpath'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays_fact 
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time,
       userID, level, s.song_id, s.artist_id, sessionId, location, userAgent
from staging_events e join staging_songs s on e.song = s.title and s.artist_name = e.artist 
where page = 'NextSong'
""")

# A workaround for upsert, which is not supported in Redshift.  
# Get the most recent instance of the user_id by finding the max ts.
user_table_insert = ("""INSERT INTO users_dim (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid, firstname, lastname, gender, level
from staging_events s1 where userid IS NOT NULL
AND ts = (SELECT MAX(ts) FROM staging_events s2 where s1.userId = s2.userId)
AND page = 'NextSong'

""")

# Adding where clause to prevent inserting duplicate records
song_table_insert = ("""INSERT INTO songs_dim (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, 
extract(year from (select timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time)) as year, duration
FROM staging_songs s join staging_events e on s.title = e.song and s.artist_name = e.artist 
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs_dim)
""")

# Adding where clause to prevent inserting duplicate records
artist_table_insert = ("""INSERT INTO artists_dim (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, location, artist_latitude, artist_longitude
from staging_songs s join staging_events e on s.title = e.song and s.artist_name = e.artist
WHERE artist_id NOT IN (SELECT DISTINCT artist_id from artists_dim)
""")

# Adding where clause to prevent inserting duplicate records
# time_table_insert = """
# INSERT INTO time_dim (start_time, hour, day, week, month, year, weekday)
# SELECT DISTINCT
# TS,
# extract(hour from TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') as hour,
# extract(day from TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') as day,
# extract(week from TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') as day,
# extract(month from TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') as month,
# extract(year from TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') as year,
# extract(dayofweek from TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second') as weekday
# from songplays_fact
# WHERE ts NOT IN (SELECT DISTINCT start_time FROM time_dim)
# """

time_table_insert = """
INSERT INTO time_dim (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
start_time,
extract(hour from start_time) as hour,
extract(day from start_time) as day,
extract(week from start_time) as day,
extract(month from start_time) as month,
extract(year from start_time) as year,
extract(dayofweek from start_time) as weekday
from songplays_fact
WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time_dim)
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create,\
                        song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,\
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
