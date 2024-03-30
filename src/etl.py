import json
import logging

from src.utils import insert_artist, insert_release, filter_data, insert_listen

from src.logging_config import configure_logging

configure_logging()

# load data into duck db
def load_data(conn, file_path):
    # for printing in log that how many records were with null ids
    null_releases, null_artist, null_recording = 0, 0, 0

    # load json file
    with open(file_path, 'r') as file:
        total_records = sum(1 for _ in file)  # Count total records for progress calculation
        file.seek(0)  # Reset file pointer to the beginning
        for i, line in enumerate(file, start=1):
            try:
                if i % 5000 == 0:  # print % in log file when 500 records are inserted.
                    progress_percentage = (i / total_records) * 100
                    logging.info(f'ETL Processed record {i} of {total_records} ({progress_percentage:.2f}%)')
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
                    insert_listen(conn, listen_id, recording_id, release_id, artist_id, track_name,
                                  listened_at_datetime,
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

