#  Data Warehouse
## Introduction 

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
The analytics team is particularly interested in understanding what songs users are listening to.
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory
with JSON metadata on the songs in their app.
we will build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. 

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

the schema of songplays table

![songplays schema](https://user-images.githubusercontent.com/49722916/209399735-6cb7e770-f028-4e6c-9f6a-a96a3ce635e4.PNG)


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

- **dwh.cfg**  has all the information about AWS such as cluster, role, S3 and database which used by all different files to access AWS.

- **sql_queries.py**  has all the queries needed to both create and drop tables and crating staging tables 

- **create_tables.py**  connect to the database using information of dwh.cfg file, has two function for dropping and creating tables.


- **etl.py**  connect to the database using information of dwh.cfg file, has two function one for load staging tables and the other for inset data into our fact and dimensions tables.



## Example query and result

Here, I write a simple analytical query to get the number of free and paid songs in songplays table and the result as shown in figure.


select  level, count(  level) from songplays
group by 1

![sql1](https://user-images.githubusercontent.com/49722916/209399423-98eb9c05-d8db-4579-a086-89e51651d5f6.PNG)


