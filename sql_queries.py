# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (songplay_id serial PRIMARY KEY, 
    start_time timestamp NOT NULL, user_id integer NOT NULL, level varchar, 
    song_id varchar, artist_id varchar, session_id varchar, location varchar, 
    user_agent varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (user_id varchar PRIMARY KEY NOT NULL, 
    first_name varchar, last_name varchar, gender varchar, level varchar);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY NOT NULL, 
    title varchar, artist_id varchar, year integer, duration float);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY NOT NULL, 
    name varchar, location varchar, latitude varchar, longitude varchar);
""")

time_table_create = ("""
     CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY NOT NULL, 
     hour integer, day integer, week integer, month integer, 
     year integer, weekday integer);
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, 
    song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE
    SET level = EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING
""")

# FIND SONGS

song_select = ("""
    SELECT songs.song_id, artists.artist_id FROM songs, artists
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, 
                        artist_table_create, time_table_create]

drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, 
                      artist_table_drop, time_table_drop]