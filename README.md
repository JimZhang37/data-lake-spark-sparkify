# Project: Data Lake
## My Introduction
A spark cluster is used in this project to transform two raw datasets into a fact/dimension star schema datasets. The raw datasets is provided by Udacity, located in its S3 bucket. The target bucket should be provided by me.


## Introduction Given By Udacity
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

Project Description
In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Datasets
It's the same datasets, at least the schema and description wise, as privious datasets provided in Data Warehouse project.

# Schema of star model
## Fact Table
- songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
## Dimension Tables
- users - users in the app
    - user_id, first_name, last_name, gender, level
- songs - songs in music database
    - song_id, title, artist_id, year, duration
- artists - artists in music database
    - artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

# Files and folders in the project

- etl.py reads data from S3, processes that data using Spark, and writes them back to S3
- dl.cfg contains your AWS credentials
- README.md provides discussion on your process and decisions
- main.ipynb is used to try new concepts and python scripts. It's not used in the final execution.

# How to Run the project
If your S3 bucket, which will be used to save outputs, and your spark cluster from EMR is in the same AWS account, you don't need AWS credentials.
1. create a new S3 bucket in US-west-2. It can be private.
2. launch your EMR cluster in US-west-2
3. upload your etl.py file to EMR master node's terminal and run it by the command `python etl.py`
4. check in your bucket that the outputs files are created for you.

Incase your spark cluster is not in the AWS or not share the same account with output files' S3 bucket, you need to configure your AWS credentils in dl.cfg and make sure this file is located in the same folder when you run `python etl.py`. Since the spark session has configured to use your AWS credentials to access the bucket, you don't need to worry the details.

# Your spark environment
The spark environment can be in your local computer or a distributed mode. Make sure you have the following softwares installed:
1. spark 2.45 **I guess spark 2.0 is also ok**
2. notebook
3. python
4. pyspark

