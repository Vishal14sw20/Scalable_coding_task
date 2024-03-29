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
- Docker (if you want to create docker image)

## Setup

Clone Repository 
```
git clone https://github.com/Vishal14sw20/Scalable_coding_task.git
```

Unzip `data/dataset.zip` file inside data folder.
```
unzip data/dataset.zip -d data/
```
> **Note:** if unzip is not installed then install it with brew install unzip
```
brew install unzip
```

### Running Locally on MacOs

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

you can see results of it in pipeline.log file.

### Creating Docker Image

Building image.
```
docker build -t scalable-capital .
```

Running container.
```
docker run scalable-capital
```

copying data into your local host directory.
```
docker cp <containerIdOrName>:/app/ /local_directory
```

This process will take some time to generating log file. meanwhile, you can check my sql queries in `analysis.py`

Finally, move to that directory and check the results in log file.
```
cat pipeline.log
```


### Results 

you can see results of my code in `pipeline.log` file. It takes little time in running.
All Analysis table will be also visible there.

### Conclusion

I am really looking forward for discussion on it. There were many things I wanted to implement in this. But I guess this is enough solution.

I got very short time on working this task. It was hard to work in week-days.
If I had a more time (week-ends :-p), 

- I could have created separate sql files for queries.
- More Error handling.
- Instead of storing data in root directory I could have stored it somewhere else and load it.
- For analysis setting up jupyter notebook. So, you can see queries with result there.
- config file for source and destination database , instead of hard coding.
- Setting up docker-compose file


