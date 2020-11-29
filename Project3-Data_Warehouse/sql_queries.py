import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
                                artist      VARCHAR(255), 
                                auth        VARCHAR(25), 
                                first_name  VARCHAR(25), 
                                gender      VARCHAR(1), 
                                itemInSession smallint, 
                                lastName    VARCHAR(25), 
                                length      REAL, 
                                level       VARCHAR(8), 
                                location    VARCHAR(255), 
                                method      VARCHAR(8),
                                page        VARCHAR(25), 
                                registration double precision, 
                                sessionId   integer,
                                song        VARCHAR(255), 
                                status      integer,
                                ts          bigint, 
                                userAgent   VARCHAR(255), 
                                userId      integer);""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs        integer, 
                                artist_id        VARCHAR(25),
                                artist_latitude  real,
                                artist_longitude real, 
                                artist_location  VARCHAR(255), 
                                artist_name      VARCHAR(255), 
                                song_id          VARCHAR(25), 
                                title            VARCHAR(255), 
                                duration         real, 
                                year             integer);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id  INTEGER IDENTITY(0,1)  PRIMARY KEY,
                                start_time   TIMESTAMP             NOT NULL, 
                                user_id      integer               NOT NULL  REFERENCES users (user_id),
                                level        VARCHAR(8), 
                                song_id      VARCHAR(25)                     REFERENCES songs (song_id), 
                                artist_id    VARCHAR(25)                     REFERENCES artists (artist_id), 
                                session_id   integer,
                                location     VARCHAR(255),
                                user_agent   VARCHAR(255));""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                user_id      integer  PRIMARY KEY, 
                                first_name   VARCHAR(25), 
                                last_name    VARCHAR(25),
                                gender       VARCHAR(1), 
                                level        VARCHAR(8));""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                song_id      VARCHAR(25)  PRIMARY KEY, 
                                title        VARCHAR(255),
                                artist_id    VARCHAR(25), 
                                year         integer,
                                duration     real);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id    VARCHAR(25)  PRIMARY KEY, 
                                name         VARCHAR(255), 
                                location     VARCHAR(255), 
                                lattitude    real,
                                longitude    real);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                start_time   TIMESTAMP  PRIMARY KEY, 
                                hour         integer, 
                                day          integer, 
                                week         integer,
                                month        integer,
                                year         integer, 
                                weekday      integer);""")

# STAGING TABLES
staging_events_copy = (""" copy staging_events from {}
                           credentials 'aws_iam_role={}' 
                           compupdate off region 'us-west-2' 
                           FORMAT AS JSON {} """).format(config['S3'].get('LOG_DATA'),
                           config['IAM_ROLE'].get('ARN').strip("'"),
                           config['S3'].get('LOG_JSONPATH'))

staging_songs_copy = ("""  copy staging_songs from {}
                           credentials 'aws_iam_role={}'
                           compupdate off region 'us-west-2'
                           FORMAT AS JSON 'auto'""").format(config['S3'].get('SONG_DATA'),
                           config['IAM_ROLE'].get('ARN').strip("'"))

songplay_table_insert = (""" INSERT INTO songplays (
                             start_time, user_id, level, song_id, 
                             artist_id, session_id, location, user_agent)   
                             SELECT timestamp 'epoch' + (e.ts / 1000) * interval '1 second'  as start_time,
                             e.userId, e.level, s.song_id, s.artist_id, 
                             e.sessionId, e.location, e.userAgent
                             FROM staging_events e
                             JOIN staging_songs s ON (e.song = s.title)
                             AND e.artist = s.artist_name 
                             AND e.length = s.duration 
                             where e.page = 'NextSong';""")


user_table_insert = (""" INSERT INTO users (
                        user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT  userId, first_name, lastName, gender, level FROM staging_events 
                        WHERE userid IS NOT NULL;""")

song_table_insert = (""" INSERT INTO songs (
                        song_id, title, artist_id, year, duration)
                        SELECT song_id, title, artist_id, year, duration FROM staging_songs 
                        WHERE song_id IS NOT NULL;""")

artist_table_insert = (""" INSERT INTO artists (artist_id, name,
                       location, lattitude, longitude)
                       SELECT DISTINCT artist_id, artist_name, artist_location,
                       artist_latitude, artist_longitude
                       FROM staging_songs
                       WHERE artist_id IS NOT NULL; """)


time_table_insert = (""" INSERT INTO time (start_time, hour, day,
                     week, month, year, weekday)
                     SELECT  start_time as ts, 
                     extract(hour from ts), 
                     extract(day from ts),
                     extract(week from ts), 
                     extract(month from ts),
                     extract(year from ts), 
                     extract(weekday from ts)
                     FROM (SELECT DISTINCT start_time FROM songplays)
                     WHERE ts IS NOT NULL; """)

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]