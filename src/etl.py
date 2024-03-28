import json
import logging

from src.utils import insert_artist, insert_release, filter_data, insert_listen
from src.database import create_database

logging.basicConfig(filename='pipeline.log', level=logging.INFO)


def load_data(file_path):
    # create database if not exists, or load it if it is already there.
    conn = create_database()
    # for printing in log that how many records were with null ids
    null_releases, null_artist, null_recording = 0, 0, 0

    # load json file
    with open(file_path, 'r') as file:
        for line in file:
            try:
                # filter data from json line.
                listen_id, recording_id, release_id, release_name, artist_id, artist_name, track_name, listened_at_datetime, user_name = filter_data(
                    line)

                # Records with not null release_id will be saved in Release table.
                if release_id is not None:
                    # Insert release if not exists
                    insert_release(conn, release_id, release_name, artist_id)
                else:
                    # count records with null release_id
                    null_releases += 1
                    # if needed: we can append those lines (which contain null release_id) in log file or separate
                    # log file can be created.
                    # for simplicity, I did not add them in current logging file.

                # Insert artist if not exists
                if artist_id is not None:
                    insert_artist(conn, artist_id, artist_name)
                else:
                    null_artist += 1

                # Insert listen
                if listen_id is not None:
                    insert_listen(conn, listen_id, recording_id, release_id, artist_id, track_name, listened_at_datetime,
                                  user_name)
                else:
                    null_recording += 1

            except json.JSONDecodeError:
                logging.warning(f"Error parsing line: {line}")
            except Exception as e:
                logging.error(f"Error inserting data: {e}")

    logging.info(f" ETL Total number of Null Recordings in source:  {null_recording}")
    logging.info(f" ETL Total number of Null Releases in source:  {null_releases}")
    logging.info(f" ETL Total number of Null Artists in source::  {null_artist}")
    logging.info(f" Data Loaded successfully in database")

    conn.close()


if __name__ == "__main__":
    load_data('data/dataset.txt')
