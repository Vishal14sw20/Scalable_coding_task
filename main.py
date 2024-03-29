import os
import logging

from src.analysis import analysis
from src.etl import load_data
from src.logging_config import configure_logging

configure_logging()

from src.database import create_database

# Get the absolute path of the directory containing the current Python script
current_dir = os.path.dirname(os.path.abspath(__file__))


source_path = os.path.join(current_dir, 'data/dataset.txt')

if __name__ == "__main__":
    # create database if not exists, or load it if it is already there.
    conn = create_database()

    # load data
    load_data(conn, source_path)


    analysis(conn)

    logging.info("Sab theek he")