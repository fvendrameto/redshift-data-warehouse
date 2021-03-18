from config import CONFIG

# Events staging table queries
STAGING_EVENTS_TABLE_CREATE = (
    """
    CREATE TABLE events_staging (
        artist_name VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender CHAR(1),
        item_in_session INT,
        last_name VARCHAR,
        length DECIMAL,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        session_id INT,
        title VARCHAR,
        status INT,
        timestamp BIGINT,
        user_agent VARCHAR,
        user_id INT
    );
    """
)

STAGING_EVENTS_COPY = (
    """
    COPY events_staging FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON '{}'
    REGION 'us-west-2';
    """
).format(CONFIG['S3']['LOG_DATA'],
         CONFIG['IAM_ROLE']['ARN'],
         CONFIG['S3']['LOG_JSONPATH'])

STAGING_EVENTS_TABLE_DROP = "DROP TABLE IF EXISTS events_staging;"

# Songs staging table queries
STAGING_SONGS_TABLE_CREATE = (
    """
    CREATE TABLE songs_staging (
        num_songs INT,
        artist_id VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL,
        location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration DECIMAL,
        year INT
    );
    """
)

STAGING_SONGS_COPY = (
    """
    COPY songs_staging FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    REGION 'us-west-2';
    """
).format(CONFIG['S3']['SONG_DATA'], CONFIG['IAM_ROLE']['ARN'])

STAGING_SONGS_TABLE_DROP = "DROP TABLE IF EXISTS songs_staging;"

# Songplays table queries
SONGPLAY_TABLE_CREATE = (
    """
    CREATE TABLE songplays (
        songplay_id BIGINT IDENTITY(0,1),
        start_time BIGINT SORTKEY REFERENCES time,
        user_id INT REFERENCES users,
        song_id VARCHAR DISTKEY REFERENCES songs,
        artist_id VARCHAR REFERENCES artists,
        session_id BIGINT,
        location VARCHAR,
        user_agent VARCHAR,
        PRIMARY KEY (songplay_id)
    );
    """
)

SONGPLAY_TABLE_INSERT = (
    """
    INSERT INTO songplays (
        start_time, user_id, song_id, artist_id, session_id, location,
        user_agent
    )
    SELECT ES.timestamp, ES.user_id, SS.song_id, SS.artist_id, ES.session_id,
           ES.location, ES.user_agent
    FROM events_staging ES
    JOIN songs_staging SS
    ON (ES.artist_name = SS.artist_name AND ES.title = SS.title)
    WHERE ES.page = 'NextSong';
    """
)

SONGPLAY_TABLE_DROP = "DROP TABLE IF EXISTS songplays;"

# Songs table queries
SONG_TABLE_CREATE = (
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR DISTKEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL REFERENCES artists,
        year INT,
        duration DECIMAL,
        PRIMARY KEY (song_id)
    );
    """
)

SONG_TABLE_INSERT = (
    """
    BEGIN TRANSACTION;

    INSERT INTO songs (
        SELECT song_id, title, artist_id, year, duration FROM (
            SELECT song_id, title, artist_id, year, duration,
                   ROW_NUMBER() OVER (
                       PARTITION BY song_id ORDER BY song_id
                    ) as row_number
            FROM songs_staging
        )
        WHERE row_number = 1
    );

    UPDATE songs S SET
    song_id = SS.song_id,
    title = SS.title,
    artist_id = SS.artist_id,
    year = SS.year,
    duration = SS.duration
    FROM songs_staging SS
    WHERE S.song_id = SS.song_id;

    END TRANSACTION;
    """
)

SONG_TABLE_DROP = "DROP TABLE IF EXISTS songs"

# Artists table queries
ARTIST_TABLE_CREATE = (
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR NOT NULL,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude DECIMAL,
        longitude DECIMAL,
        PRIMARY KEY (artist_id)
    )
    DISTSTYLE all;
    """
)

ARTIST_TABLE_INSERT = (
    """
    BEGIN TRANSACTION;

    INSERT INTO artists (
        SELECT artist_id, artist_name, location, latitude, longitude FROM (
            SELECT artist_id, artist_name, location, latitude, longitude,
                   ROW_NUMBER() OVER (
                       PARTITION BY artist_id ORDER BY artist_id
                    ) as row_number
            FROM songs_staging
        )
        WHERE row_number = 1
    );

    UPDATE artists A SET
    artist_id = SS.artist_id,
    name = SS.artist_name,
    location = SS.location,
    latitude = SS.latitude,
    longitude = SS.longitude
    FROM songs_staging SS
    WHERE A.artist_id = SS.artist_id;

    END TRANSACTION;
    """
)

ARTIST_TABLE_DROP = "DROP TABLE IF EXISTS artists"

# Users table queries
USER_TABLE_CREATE = (
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR NOT NULL,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender CHAR(1),
        level VARCHAR,
        PRIMARY KEY (user_id)
    )
    DISTSTYLE all;
    """
)

USER_TABLE_INSERT = (
    """
    BEGIN TRANSACTION;

    INSERT INTO users (
        SELECT user_id, first_name, last_name, gender, level FROM (
            SELECT user_id, first_name, last_name, gender, level,
                   ROW_NUMBER() OVER (
                       PARTITION BY user_id ORDER BY user_id
                    ) as row_number
            FROM events_staging
            WHERE page = 'NextSong'
        )
        WHERE row_number = 1
    );

    UPDATE users U SET
    user_id = ES.user_id,
    first_name = ES.first_name,
    last_name = ES.last_name,
    gender = ES.gender,
    level = ES.level
    FROM events_staging ES
    WHERE U.user_id = ES.user_id
    AND ES.page = 'NextSong';

    END TRANSACTION;
    """
)

USER_TABLE_DROP = "DROP TABLE IF EXISTS users;"

# Time table queries
TIME_TABLE_CREATE = (
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT NOT NULL,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday VARCHAR,
        PRIMARY KEY (start_time)
    );
    """
)

TIME_TABLE_INSERT = (
    """
    INSERT INTO time
    SELECT TS.timestamp,
           EXTRACT(hour FROM TS.start_time),
           EXTRACT(day FROM TS.start_time),
           EXTRACT(week FROM TS.start_time),
           EXTRACT(month FROM TS.start_time),
           EXTRACT(year FROM TS.start_time),
           EXTRACT(weekday FROM TS.start_time)
    FROM (
        SELECT DISTINCT ES.timestamp,
            TIMESTAMP 'epoch' + ES.timestamp/1000 * interval '1 s' as start_time
        FROM events_staging ES
    ) TS;
    """
)

TIME_TABLE_DROP = "DROP TABLE IF EXISTS time;"

# Query lists
CREATE_TABLE_QUERIES = [
    STAGING_EVENTS_TABLE_CREATE,
    STAGING_SONGS_TABLE_CREATE,
    ARTIST_TABLE_CREATE,
    SONG_TABLE_CREATE,
    USER_TABLE_CREATE,
    TIME_TABLE_CREATE,
    SONGPLAY_TABLE_CREATE
]

DROP_TABLE_QUERIES = [
    STAGING_EVENTS_TABLE_DROP,
    STAGING_SONGS_TABLE_DROP,
    SONG_TABLE_DROP,
    ARTIST_TABLE_DROP,
    USER_TABLE_DROP,
    TIME_TABLE_DROP,
    SONGPLAY_TABLE_DROP
]

COPY_TABLE_QUERIES = [
    STAGING_EVENTS_COPY,
    STAGING_SONGS_COPY
]

INSERT_TABLE_QUERIES = [
    SONG_TABLE_INSERT,
    ARTIST_TABLE_INSERT,
    USER_TABLE_INSERT,
    TIME_TABLE_INSERT,
    SONGPLAY_TABLE_INSERT
]
