# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays 
(songplay_id int, start_time int, user_id int, level int, song_id text, artist_id text, session_id int, 
location text, user_agent text)
""")

user_table_create = ("""
create table if not exists users (user_id int primary key, first_name text, last_name text, gender text, level text)
""")

song_table_create = ("""
create table if not exists songs (song_id text primary key, title text, artist_id text, year int, duration numeric)
""")

artist_table_create = ("""
create table if not exists artists (artist_id text primary key, name text, location text, latitude numeric, longitude numeric)
""")

time_table_create = ("""
create table if not exists time 
(start_time timestamp primary key, hour int, day int, week int, month int, year int, weekday int)
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
values (%s, %s, %s, %s, %s)
on conflict (user_id) do nothing
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration) 
values (%s, %s, %s, %s, %s)
on conflict (song_id) do update set 
(title, artist_id, year, duration) = (excluded.title, excluded.artist_id, excluded.year, excluded.duration)
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude) 
values (%s, %s, %s, %s, %s)
on conflict (artist_id) do update set 
(location, latitude, longitude) = (excluded.location, excluded.latitude, excluded.longitude)
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
values (%s, %s, %s, %s, %s, %s, %s)
on conflict (start_time) do nothing
""")

# FIND SONGS

song_select = ("""
select a.artist_id, s.song_id 
from artists a, songs s 
where a.artist_id = s.artist_id
and a.name = %s
and s.title = %s
and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [
	songplay_table_create, user_table_create, song_table_create, artist_table_create,
	time_table_create,
]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
