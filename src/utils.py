import json
from datetime import datetime
import duckdb

# return filtered data from json line


def filter_data(line):
    # filter data from json line.
    data = json.loads(line)
    recording_id = data['recording_msid']
    artist_id = data['track_metadata']['additional_info']['artist_msid']
    artist_name = data['track_metadata']['artist_name']
    release_id = data['track_metadata']['additional_info']['release_msid']
    track_name = data['track_metadata']['track_name']
    release_name = data['track_metadata']['release_name']
    user_name = data['user_name']
    listened_at = data['listened_at']

    # converting timestamp into data time format
    listened_at_datetime = datetime.fromtimestamp(listened_at)

    # User can listen one song again and again.
    # that is why we created primary key for listen table by combination of recording_id and listened_at
    listen_id = recording_id + '_' + str(listened_at_datetime)

    return listen_id, recording_id, release_id, release_name, artist_id, artist_name, track_name, listened_at_datetime, user_name


# it inserts new artist in table, if artist id come again it updates the record
def insert_artist(conn, artist_id, artist_name):
    conn.execute("""
                INSERT INTO artists (id, artist_name)
                VALUES (?, ?)
                ON CONFLICT (id) DO UPDATE SET artist_name = excluded.artist_name;
            """, (artist_id, artist_name))


# it inserts new release in table, if release id come again it updates the record
def insert_release(conn, release_id, release_name, artist_id):
    conn.execute("""
        INSERT INTO releases (id, release_name, artist_id)
        VALUES (?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET release_name = excluded.release_name;
    """, (release_id, release_name, artist_id))


# it inserts new listen in table, if listen id come again it updates the record
def insert_listen(conn, listen_id, recording_id, release_id, artist_id, track_name, listened_at_datetime, user_name):
    conn.execute("""
        INSERT INTO listens (id, recording_id, release_id, artist_id, track_name, listened_at, user_name)
        VALUES (?, ?, ?, ?, ?, ?,?) ON CONFLICT (id) DO UPDATE 
        SET listened_at = excluded.listened_at,
        release_id = excluded.release_id,
        artist_id = excluded.artist_id,
        user_name = excluded.user_name,
        track_name = excluded.track_name,

    """, (listen_id, recording_id, release_id, artist_id, track_name, listened_at_datetime, user_name))


# extra work if we want to store corrupted data.
def insert_corrupt_data(conn, id, recording_id, release_id, release_name, artist_id, artist_name, track_name,
                        listened_at_datetime, user_name):
    conn.execute("""
        INSERT INTO corrupt_data (id, recording_id, release_id, release_name, artist_id, artist_name, track_name, listened_at, user_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO Nothing 
    """, (
        id, recording_id, release_id, release_name, artist_id, artist_name, track_name, listened_at_datetime,
        user_name))
