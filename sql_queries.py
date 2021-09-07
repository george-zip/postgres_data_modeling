# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

# song_id and artist_id have to accept nulls
# as we don't have all of the artists and songs in our data directory
songplay_table_create = ("""
create table if not exists songplays (
songplay_id serial primary key, 
start_time bigint not null references time(start_time), 
user_id int not null references users(user_id), 
level text not null, 
song_id text null references songs(song_id), 
artist_id text null references artists(artist_id), 
session_id int not null, 
location text not null, 
user_agent text not null
)
""")

user_table_create = ("""
create table if not exists users (
user_id int primary key, 
first_name text not null, 
last_name text not null, 
gender text not null, 
level text not null
)
""")

artist_table_create = ("""
create table if not exists artists (
artist_id text primary key, 
name text not null, 
location text not null, 
latitude numeric not null, 
longitude numeric not null
)
""")

song_table_create = ("""
create table if not exists songs (
song_id text primary key, 
title text not null, 
artist_id text not null references artists(artist_id), 
year int not null, 
duration numeric not null
)
""")

time_table_create = ("""
create table if not exists time 
(
start_time bigint primary key, 
hour int not null, 
day int not null, 
week int not null, 
month int not null, 
year int not null, 
weekday int not null
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values (%s, %s, %s, %s, %s, %s, %s, %s)
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
select s.song_id, a.artist_id 
from songs s, artists a 
where s.artist_id = a.artist_id
and s.title = %s
and a.name = %s
and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [
	user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create
]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
