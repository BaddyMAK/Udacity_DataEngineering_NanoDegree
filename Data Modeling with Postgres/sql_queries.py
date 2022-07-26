# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
  (
     songplay_id SERIAL PRIMARY KEY NOT NULL,
     start_time  TIMESTAMP NOT NULL,
     user_id     INT NOT NULL,
     level       VARCHAR,
     song_id     VARCHAR,
     artist_id   VARCHAR,
     session_id  INT,
     location    TEXT,
     user_agent  TEXT
  );""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users
  (
      user_id int PRIMARY KEY NOT NULL,
      first_name VARCHAR,
      last_name VARCHAR,
      gender CHAR,
      level VARCHAR
   );""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
   (
       song_id VARCHAR PRIMARY KEY NOT NULL,
       title TEXT NOT NULL,
       artist_id VARCHAR,
       year INT,
       duration FLOAT NOT NULL
    );""")


artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
   (
       artist_id VARCHAR PRIMARY KEY NOT NULL,
       artist_name VARCHAR NOT NULL,
       artist_location TEXT,
       artist_latitude FLOAT,
       artist_longitude FLOAT
    );""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
   (
       start_time TIMESTAMP PRIMARY KEY NOT NULL,
       hour INT,
       day INT,
       week INT,
       month INT ,
       year INT,
       weekday INT
    );""")


# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id) WHERE songplay_id IS NULL 
    DO UPDATE SET user_id = EXCLUDED.user_id 
   """)

user_table_insert = ("""
   INSERT INTO users (user_id, first_name, last_name, gender, level)
   VALUES (%s, %s, %s, %s, %s)
   ON CONFLICT (user_id) 
   DO UPDATE SET user_id = EXCLUDED.user_id
   """)

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING  
   """)

artist_table_insert = ("""
    INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING
    """)


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING""")

# FIND SONGS
#to find the song ID and artist ID based on the title, artist name, and duration of a song.

song_select = ("""SELECT song_id, songs.artist_id 
    FROM (songs JOIN artists ON songs.artist_id = artists.artist_id)
    WHERE songs.title=%s AND artists.artist_name=%s AND songs.duration=%s""")



# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]