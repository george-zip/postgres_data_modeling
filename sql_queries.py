# DROP TABLES

log_data_staging_import_table_drop = "drop table if exists log_data_staging"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

# staging table for log data before copying into destination tables
# we're reusing the raw field names from the json file
log_data_staging_import_table_create = ("""
create unlogged table if not exists log_data_staging (
num int,
artist text,
auth text,
firstName text,
gender text,
itemInSession int,
lastName text,
length text,
level text,
location text,
method text,
page text,
registration text,
sessionId int,
song text,
status int,
ts bigint,
userAgent text,
userId text
);
""")

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

time_table_insert_from_staging = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select distinct ts,
date_part('hour', to_timestamp(TRUNC(ts / 1000))) as hour,
date_part('day', to_timestamp(TRUNC(ts / 1000))) as day,
date_part('week', to_timestamp(TRUNC(ts / 1000))) as week,
date_part('month', to_timestamp(TRUNC(ts / 1000))) as month,
date_part('year', to_timestamp(TRUNC(ts / 1000))) as year,
date_part('dow', to_timestamp(TRUNC(ts / 1000))) as weekday
from log_data_staging
where page = 'NextSong';
""")

user_table_insert_from_staging = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct on (userId) cast(userId as integer), firstName, lastName, gender, level
from log_data_staging
where userId != '';
""")

songplays_table_insert_from_staging = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select st.ts, cast(st.userId as integer), st.level, s.song_id, a.artist_id, st.sessionid,
       st.location, st.useragent
from log_data_staging st
left outer join songs s
on st.song = s.title and cast(st.length as numeric(10,3)) = cast(s.duration as numeric(10,3))
left outer join artists a
on st.artist = a.name
where st.userId != ''
and page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [
	user_table_create, artist_table_create, song_table_create,
	time_table_create, songplay_table_create, log_data_staging_import_table_create
]
drop_table_queries = [
	songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop,
	time_table_drop, log_data_staging_import_table_drop
]
