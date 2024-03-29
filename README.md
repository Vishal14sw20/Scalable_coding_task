# Scalable Capital Data Engineering Task

This project include task of creating ETL pipeline and Data Analysis

## Project Structure

- `data/`: Contains the source dataset(s).
- `src/`: Contains the source code for the project.
  - `database.py`: Initializes and configures the DuckDB database.
  - `utils.py`: Contains utility libraries and functions used across the project.
- `pipeline.log`: This log file will be created when you run python script.
- `listenbrainz.db`: This file.
- `main.py`: The main ETL script that orchestrates the data processing pipeline.

- `requirements.txt`: This file.
- `README.md`: This file.

## Prerequisites

- Python 3.x installed on your system.
- Git

## Setup

```
git clone git@github.com:Vishal14sw20/Scalable_coding_task.git
```
copy data from source and unzip it and place in data folder

```
cd Scalable_coding_task
```
```
python -m venv venv
```
```
source venv/bin/activate
```
```
pip install -r requirements.txt
```
```
python src/etl.py
```
```
jupyter notebook
```

jupyter notebook



