## Project: Data Modeling with Postgres

### Background

This is the data modeling with postgres project that is part of the Udacity Data Engineering Nanodegree. The goal is to 
design a Postgres schema that facilitates analytical queries for a music streaming application, as well a data pipeline 
for populating it. The client would like to understand which users play what songs and don't have a good way to pull it 
all together. The tables should be optimized for queries on song plays and be able to provide context such as users, 
songs, artists and timing.

JSON log files provide the source of raw song play data and other JSON files provide metadata about the music.

### Design

A star schema with a fact table containing song plays seems like a natural fit. Dimension tables will categorize users,
timing, songs and artists. 

#### Primary Keys

| Table | Primary Key |
| --- | --- |
| songplays | songplay_id |
| users | user_id |
| artists | artist_id |
| songs | song_id |
| time | start_time |

#### Foreign Keys

| Table | Primary Key | References |
| --- | --- | --- |
| songplays | start_time | time |
| songplays | user_id | users |
| songplays | artist_id | artists |
| songplays | song_id | songs |
| songs | artist_id | artists |

![Entity Relationship](Sparkify.jpg?raw=true "SparkifyDB")

Note, songplays.song_id and songplays.artist_id may be null so participation is optional in the referencing table.

### Implementation

The project contains the following scripts:

[create_tables.py](create_tables.py) drops and creates the schema, using queries in [sql_queries.py](sql_queries.py).

[etl.py](etl.py) contains the ETL logic to populate the tables from JSON files in the [data directory](data)

[sql_queries.py](sql_queries.py) defines the SQL commands for schema creation and population.

[test_db.py](test_db.py) runs some verification queries on the populated database tables.

### Sample Queries

Which songs do users play during the week?

```sql
select s.title
from songplays sp, time t, songs s
where sp.start_time = t.start_time
and s.song_id = sp.song_id
and t.weekday not in (0, 6)
```

How many paid users that identify as male listen to Sparkify during morning commuting hours?

```sql
select count(distinct u.*)
from users u, songplays sp, time t
where  sp.user_id = u.user_id
and sp.start_time = t.start_time
and u.gender = 'M'
and u.level = 'paid'
and t.hour < 10
```