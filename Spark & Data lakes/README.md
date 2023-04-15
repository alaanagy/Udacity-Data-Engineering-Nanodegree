#  Data Lake

## Introduction 

A music streaming startup, Sparkify, has grown their user base and song database even more and want build its own data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We are going to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Project Datasets

we will work on 2 datasets in this project, song dataset and log dataset, so we will explain a breif on each of them in the upcoming lines.

### Song Dataset
It is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).Each file is in JSON format and contains metadata
about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to
two files in this dataset.

- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}
```

### Log Dataset

The second dataset consists of log files in JSON format where each file covers the users activities over a given day.
 For example, here are filepaths to two files in this dataset.
 
- log_data/2018/11/2018-11-12-events.json
- log_data/2018/11/2018-11-13-events.json


And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.
![image](https://user-images.githubusercontent.com/49722916/201693400-7f50ae13-69e3-4644-9488-5504cf326e14.png)

## Schema for Song Play Analysis

### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


### Dimension Tables

2. **users** - users in the app
    - user_id, first_name, last_name, gender, level

3. **songs** - songs in music database
    - song_id, title, artist_id, year, duration

4. **artists** - artists in music database
    - artist_id, name, location, latitude, longitude
   
5. **time** - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday


And below is the ERD of our databse.


![image](https://user-images.githubusercontent.com/49722916/201710786-b957669f-5087-4c95-b277-c19404d23857.png)



## Description of Files

- **etl.py** 
reads data from S3, processes that data using Spark, and writes them back to S3


- **dl.cfg**   contains AWS credentials

- **README.md**  provides discussion on your process and decisions