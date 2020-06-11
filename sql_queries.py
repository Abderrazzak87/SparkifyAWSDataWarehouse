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

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char(1),
        itemInSession int,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        userId int
    ) DISTKEY("song") SORTKEY("ts")
    ;
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id varchar, 
        artist_latitude numeric, 
        artist_location varchar, 
        artist_longitude numeric,
        artist_name varchar,
        duration numeric,
        num_songs int,
        song_id varchar,
        title varchar,
        year int
    ) DISTKEY("title")
    ;
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int not null IDENTITY(0,1) PRIMARY KEY, 
        start_time timestamp NOT NULL , 
        user_id int NOT NULL  , 
        song_id varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        session_id int, 
        location varchar, 
        user_agent varchar
    ) DISTKEY("song_id") SORTKEY("start_time")
    ;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int not null  PRIMARY KEY, 
        first_name varchar NOT NULL, 
        last_name varchar NOT NULL, 
        gender char(1) NOT NULL, 
        level varchar NOT NULL
    ) SORTKEY("user_id")
    ;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar not null  PRIMARY KEY, 
        title varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        year int NOT NULL, 
        duration numeric NOT NULL
    ) SORTKEY("song_id")
    ;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar not null  PRIMARY KEY, 
        name varchar distkey, 
        location varchar, 
        latitude numeric, 
        longitude numeric
    ) SORTKEY("artist_id")
    ;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp not null  PRIMARY KEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL
    ) SORTKEY("start_time")
    ;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {} 
    format as json {}
""").format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {} 
    json 'auto' compupdate off region 'us-west-2';
""").format(config.get("S3","SONG_DATA"),config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (  start_time, user_id, song_id, artist_id, session_id, location, user_agent )
    SELECT TIMESTAMP 'epoch' + s_event.ts/1000 * intERVAL '1 second' as start_time,
    s_event.userid,
    s_song.song_id,
    s_song.artist_id,
    s_event.sessionid,
    s_event.location,
    s_event.useragent
    FROM staging_events s_event inner join staging_songs s_song
    on s_song.artist_name = s_event.artist
    and s_song.title = s_event.song
    WHERE s_song.artist_name is not null
    and s_song.title is not null
    and s_event.page = 'NextSong';
    ;
""")

user_table_insert = ("""
    INSERT INTO users ( user_id, first_name, last_name, gender, level )
    SELECT DISTINCT userid,
    firstname,
    lastname,
    gender,
    level
    FROM staging_events
    WHERE userid IS NOT NULL
    ;
""")

song_table_insert = ("""
    INSERT INTO songs ( song_id, title, artist_id, year, duration )
    SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    ;
""")

artist_table_insert = ("""
    INSERT INTO artists ( artist_id, name, location, latitude, longitude )
    SELECT DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    ;
""")

time_table_insert = ("""
    INSERT INTO time ( start_time, hour, day, week, month, year, weekday )
    SELECT DISTINCT start_time,
    EXTRACT(hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(week from start_time),
    EXTRACT(month from start_time),
    EXTRACT(year from start_time),
    EXTRACT(weekday from start_time)
    FROM songplays
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]