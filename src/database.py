import duckdb
import os
import logging

logging.basicConfig(filename='pipeline.log', level=logging.INFO)


# creating 3 database here namely artist , release and listens.
# did not create track database because unique identifier for track is null in most cases.
# these queries could be also saved in different .sql files but for simplicity I kept them here.
def create_database():
    # database name
    db_file = 'listenbrainz.db'

    # create database if it not exists.
    if not os.path.exists(db_file):
        conn = duckdb.connect(db_file)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS artists (
                id VARCHAR PRIMARY KEY,
                artist_name VARCHAR
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS releases (
                id VARCHAR PRIMARY KEY,
                release_name TEXT,
                artist_id VARCHAR
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS listens (
                    id VARCHAR PRIMARY KEY,
                    recording_id VARCHAR ,
                    release_id VARCHAR,
                    artist_id VARCHAR,
                    track_name VARCHAR,
                    listened_at TIMESTAMP,
                    user_name VARCHAR
            );
        """)

        # I am not storing any data in this table for now. but it can be use-full for debugging if needed.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS corrupt_data (
                    id VARCHAR PRIMARY KEY,
                    recording_id VARCHAR,
                    release_id VARCHAR,
                    release_name VARCHAR,
                    artist_id VARCHAR,
                    artist_name VARCHAR,
                    track_name VARCHAR,
                    listened_at TIMESTAMP,
                    user_name VARCHAR,
            );
        """)
        logging.info(f"Database '{db_file}' created with tables: artists, releases, listens.")
    else:
        logging.info(f"Database '{db_file}' already exists. Skipping creation.")
        conn = duckdb.connect(db_file)
    return conn
