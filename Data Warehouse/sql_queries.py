
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
print(config)

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS stagingevents"
staging_songs_table_drop = "DROP table IF EXISTS stagingsongs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"



# CREATE TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stagingevents
  (
     artist             VARCHAR(25),
     auth               VARCHAR(25),
     firstName          VARCHAR(10),
     gender             VARCHAR(1), 
     itemInSession      INT,
     lastName           VARCHAR(10),
     length             FLOAT,
     level              VARCHAR(6),
     location           VARCHAR(25),
     method             VARCHAR(6), 
     page               VARCHAR(10), 
     registration       FLOAT, 
     sessionId          INT, 
     song               VARCHAR(25), 
     status             INT, 
     ts                 bigint,
     userAgent          VARCHAR(25), 
     userId             INT
  );""")


staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stagingsongs
  (  song_id           VARCHAR,
     num_songs         INT,
     artist_id         VARCHAR,
     artist_latitude   FLOAT,
     artist_longitude  FLOAT,
     artist_location   TEXT,
     artist_name       VARCHAR,
     title             TEXT,
     duration          FLOAT, 
     year              INT
  );""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
  (
     songplay_id    INT IDENTITY(0,1) PRIMARY KEY,
     start_time        TIMESTAMP,
     user_id           INT,
     level             VARCHAR,
     song_id           VARCHAR,
     artist_id         VARCHAR,
     session_id        INT,
     location          TEXT,
     user_agent        TEXT
  );""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
  (
      user_id int      PRIMARY KEY,
      first_name       VARCHAR,
      last_name        VARCHAR,
      gender           CHAR,
      level            VARCHAR
   );""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
   (
       song_id         VARCHAR PRIMARY KEY,
       title           TEXT,
       artist_id       VARCHAR,
       year            INT,
       duration        FLOAT
    );""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
   (
       artist_id       VARCHAR PRIMARY KEY,
       artist_name     VARCHAR,
       artist_location  TEXT,
       artist_latitude  FLOAT,
       artist_longitude  FLOAT
    );""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
   (
       start_time TIMESTAMP PRIMARY KEY,
       hour              INTEGER,
       day               INTEGER,
       week              INTEGER,
       month             INTEGER,
       year              INTEGER,
       weekday           INTEGER
    );""")



# STAGING TABLES

staging_events_copy = ("""
    COPY stagingevents FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF
    region 'us-west-2'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL
    FORMAT AS JSON {}; """
                      ).format(config.get("S3",'LOG_DATA'), config.get("IAM_ROLE",'ARN'), config.get("S3",'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY stagingsongs from {}
    CREDENTIALS 'aws_iam_role={}' 
    COMPUPDATE OFF 
    region 'us-west-2'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL
    FORMAT AS JSON 'auto' ;
                    """).format(config.get("S3",'SONG_DATA'), config.get("IAM_ROLE",'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT (
    to_timestamp(cast(ste.ts/1000 as bigint)) as start_time,
    sge.userId as user_id,
    sge.level as level,
    sgs.song_id as song_id
    sgs.artist_id as artist_id, 
    sge.sessionId as session_id,
    sge.location as location, 
    sge.userAgent as user_agent
    )
    FROM stagingevents sge
    JOIN stagingsongs sgs ON sge.song = sgs.title AND sge.artist = sgs.artist_name;
   """) 
    
user_table_insert = ("""
   INSERT INTO users (user_id, first_name, last_name, gender, level)
   SELECT (userId as user_id, firstName as first_name,  lastName as last_name, gender as gender, level as level )
   FROM stagingevents
   """)#######

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT (song_id, title, artist_id, year, duration)
    FROM stagingsongs
   """) #####

artist_table_insert = ("""
    INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    FROM stagingsongs
    """)

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT (
    to_timestamp(cast(ste.ts/1000 as bigint)) as start_time, 
    EXTRACT(hour from ste.ts) as hour,
    EXTRACT(day from ste.ts) as day, 
    EXTRACT(week from ste.ts) as   
    week, EXTRACT(month from ste.ts) as month, 
    EXTRACT(year from ste.ts) as year,
    EXTRACT(weekday from ste.ts) as weekday  
    FROM stagingevents  ste""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
