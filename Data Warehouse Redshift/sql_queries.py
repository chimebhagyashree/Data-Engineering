import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
HOST= config.get("CLUSTER","HOST")
DB_NAME=config.get("CLUSTER","DB_NAME")
DB_USER= config.get("CLUSTER","DB_USER")
DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
DB_PORT = config.get("CLUSTER","DB_PORT")

ARN=config.get("IAM_ROLE","ARN")

LOG_DATA=config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stg_events(
                    stg_event_id      int IDENTITY(1,1) NOT NULL,
                    artist            varchar,
                    auth              varchar(40),
                    firstName         varchar(40),
                    gender            varchar(20),
                    itemInSession     int,
                    lastName          varchar(40),
                    length            double precision,
                    level             varchar(20),
                    location          varchar(100),
                    method            varchar,
                    page              varchar,
                    registration      double precision,
                    sessionid         int,
                    song              varchar(200),
                    status            int,
                    ts                bigint,
                    useragent         varchar(200),
                    userid            int,
                    primary     key(stg_event_id))

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_songs(
                     stg_song_id              int IDENTITY(1,1) NOT NULL,
                     num_songs                int,
                     artist_id                varchar(30),
                     artist_latitude          double precision,
                     artist_longitude         double precision,
                     artist_location          varchar(300),
                     artist_name              varchar,
                     song_id                  varchar(40),
                     title                    varchar(200),
                     duration                 double precision,
                     year                     int,
                     primary    key(stg_song_id))
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
                    songplay_id  int IDENTITY(1,1) NOT NULL,
                    start_time   timestamp,
                    user_id      int distkey,
                    level        varchar(20),
                    song_id      varchar(40) sortkey,
                    artist_id    varchar(30),
                    session_id   int,
                    location     varchar(100),
                    user_agent   varchar(200),
                    primary  key(songplay_id))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
                    user_id     int not null distkey,
                    first_name  varchar(40) not null,
                    last_name   varchar(40) not null,
                    gender      varchar(10) not null,
                    level       varchar(20) not null sortkey,
                    primary key(user_id))

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
                    song_id    varchar(40) not null,
                    title      varchar(200) not null,
                    artist_id  varchar(30) not null,
                    year       int not null sortkey,
                    duration   double precision,
                    primary key(song_id))
diststyle all
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
                    artist_id    varchar(30) not null sortkey,
                    name         varchar,
                    location     varchar(300),
                    lattitude    double precision,
                    longitude    double precision,
                    primary  key(artist_id))
diststyle all
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
                    start_time    timestamp not null sortkey,
                    hour          int not null,
                    day           int not null,
                    week          int not null,
                    month         int not null,
                    year          int not null,
                    weekday       int not null,
                    primary       key(start_time))
diststyle all
""")

# STAGING TABLES

staging_events_copy = (""" COPY stg_events 
                           FROM {}
                           iam_role {}
                           COMPUPDATE OFF region 'us-west-2'
                           TIMEFORMAT as 'epochmillisecs'
                           FORMAT AS JSON {};
                          
""").format(LOG_DATA,ARN,LOG_JSONPATH) 

staging_songs_copy = (""" COPY stg_songs 
                           FROM {}
                           iam_role {}
                           COMPUPDATE OFF region 'us-west-2'
                           JSON 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = (""" 
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
           se.userid                                            AS user_id,
           se.level                                             AS level,
           ss.song_id                                           AS song_id,
           ss.artist_id                                         AS artist_id,
           se.sessionid                                         AS session_id,
           se.location                                          AS location,
           se.useragent                                         AS user_agent
    FROM stg_events se
    LEFT JOIN stg_songs ss                 
    ON ss.title = se.song AND
       ss.duration = se.length AND
       ss.artist_name = se.artist
    WHERE se.page='NextSong'
""")

user_table_insert = (""" INSERT INTO users(
                               user_id, 
                               first_name,
                               last_name,
                               gender, 
                               level)
                         SELECT DISTINCT
                               userid,
                               firstName,
                               lastName,
                               gender,
                               level
                         FROM stg_events
                         WHERE page='NextSong'
                         AND userid IS NOT NULL;
""")

song_table_insert = (""" INSERT INTO songs(
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration)
                         SELECT DISTINCT
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration
                         FROM stg_songs
                         WHERE song_id IS NOT NULL;
""")

artist_table_insert = (""" INSERT INTO artists(
                                artist_id,
                                name,
                                location,
                                lattitude,
                                longitude)
                           SELECT DISTINCT
                                artist_id,
                                artist_name,
                                artist_location,
                                artist_latitude,
                                artist_longitude
                            FROM stg_songs
                            WHERE artist_id IS NOT NULL ;
""")

time_table_insert = (""" INSERT INTO time(
                                start_time,
                                hour,
                                day, 
                                week,
                                month,
                                year,
                                weekday)
                        SELECT  DISTINCT
                                timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
                                EXTRACT(HOUR FROM start_time)                        AS hour,
                                EXTRACT(DAY FROM start_time)                         AS day,
                                EXTRACT(WEEK FROM start_time)                        AS week,
                                EXTRACT(MONTH FROM start_time)                       AS month,
                                EXTRACT(YEAR FROM start_time)                        AS year,
                                EXTRACT(DOW FROM start_time)                         AS weekday
                        FROM stg_events se
                        WHERE se.page='NextSong'
                        AND se.userid IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
