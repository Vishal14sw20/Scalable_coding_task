# Scalable Capital Data Engineering Task

This project include task of creating ETL pipeline and Data Analysis


## Overview
Some points I want to highlight for this case study.
- I created seperate tables for artist, release and listens. however in this case study it was not needed. but its scalable solution. 
- The source contained records with null release_ids. I exclude them from release table but they are store in listen table.
- User can listen one song again that is why for listen table I created primary key with combination of **recording_id** and **listened_at**.
- I did not create track table because most of **track_mbid** were null.
- In ETL pipeline if duplicate data come then it updates the value and if corrupted data come, It will be printed in log file.


## Project Structure

- `data/`: Contains the source dataset(s).
- `src/`: Contains the source code for the project.
  - `database.py`: Initializes and configures the DuckDB database.
  - `utils.py`: Contains utility libraries and functions used across the project.
  - `etl.py`: load data into duck db.
  - `analysis.py`: contain all analysis queries.
- `pipeline.log`: This log file will be created when you run python script. it shows any bug in pipeline and result tables of analysis
- `listenbrainz.db`: Duck db database file, created when running python script.
- `main.py`: The main ETL script that orchestrates the data processing pipeline.
- `Dockerfile`: For creating docker image.
- `requirements.txt`: This file.
- `README.md`: This file.

## Prerequisites

- Python 3.9 installed on your system.
- Git
- Docker

## Setup

Clone Repository 
```
git clone git@github.com:Vishal14sw20/Scalable_coding_task.git
```
Download data from [Test Datasets](https://drive.google.com/drive/folders/1wnAXYL4BtchW6J8C8YaqOOo9ba6NFOva)
 and unzip it. Move unziped file into data folder of root directory.

## Running Locally on MacOs

Go to that repository.
```
cd Scalable_coding_task
```
Create virtual_env, activate it and install requirements.
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
Run python script
```
python3 main.py
```

## Creating Docker Image

Building image.
```
docker build -t scalable-capital .
```

Running container.
```
docker build -t scalable-capital .
```




