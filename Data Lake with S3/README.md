1- Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

A music streaming startup, Sparkify, is collecting their data related to songs and user activities on the app. The stored dataset locally has grown and Sparkify engineers has decided to move or migrate their data into datawarehouse on AWS s3. As, the user base and song database have grown even more, Sparkify decided to move their data warehouse into a data lake knowing while keeping  all data in S3.


Actually, the stored data in s3 has two different json directories:

- One directory contains JSON logs on user activity on the app under the next path : s3://udacity-dend/song_data
- The second directory contains JSON metadata on the songs in their app under the next path: s3://udacity-dend/log_data

The Sparkify's analytics team wants to continue finding insights into what songs their users are listening to. That's why as their data engineer, I had built an ETL pipeline that extracts their data from S3, processes them using Spark, and loadsthe data back into S3 as a set of dimensional and fact tables (via adopting the star schema). I



2- State and justify your schema design and ELT pipeline.

The used schema for this project is the star schema therefore our files stored in two json directories in S3 were converted into :

- Four dimension tables : users, songs, artists and time
- One fact table : songplays

To create each table, I had follwed the next steps:

- Reads the dataframe either from Song data path or Log data paths
- Select the required columns to create each table (fro example for user table I have selected :user_id, first_name, last_name, gender, level.
- Write this table into a parquet file and store it again into s3 under output_data path


The star schema that uses a single large fact table (songplays) to store transactional or measured data, and one or more smaller dimensional tables that store attributes about the data.

The analytics teams wants to implement this architecture to understand what songs users are listening to. Therefore, we have used table to describe songs details, another table to describe users behaviors and the third table stores information details about artists. The fourth table : songplays resumes relationship between users, songs and artists. However, The timetable is useful for analysis during certain periods like weekend or weekdays, holidays etc.

- The ETL pipeline consists on :
        - Extracting the data from the json Repositories in S3 t
        - Processes and transform this data using Spark to satisfy the requirements of each of the five tables (selecting the correct columns for each table)
        - Insert or load transformed data again to s3 into analytics tables using the parquet format.

In the dl.cfg file we are going to put our aws access key id and  aws secret access key. then, we run the etl.py script that uses this credentials and runs a number of functions and modules written in python that extracts our data from s3 and process them using spark.

To run this code all what you need is to write python etl.py in your terminal and tables are going to be created 