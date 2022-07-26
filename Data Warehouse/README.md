1- Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

A music streaming startup, Sparkify, is collecting their data related to songs and user activities on the app. The stored dataset locally has grown and Sparkify engineers has decided to move or migrate their data into AWS s3. As, the user base and song database have grown even more, Sparkify decided to move their data warehouse to a data lake knowing that all data will always reside in S3.


Actually, the stored data in s3 has two different json directories:

- One directory contains JSON logs on user activity on the app under the next path : s3://udacity-dend/song_data
- The second directory contains JSON metadata on the songs in their app under the next path: s3://udacity-dend/log_data

The Sparkify's analytics team wants to continue finding insights into what songs their users are listening to. That's why as their data engineer, I had built an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional and fact tables (via adopting the star schema). I



you I have to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables.

2- State and justify your database schema design and ETL pipeline.

The used schema for this project is the star schema therefore our files stored in two json directories in S3 were converted into :

- Four dimension tables : users, songs, artists and time
- One fact table : songplays
- Two staging tables stagingevents and stagingsongs

The star schema that uses a single large fact table (songplays) to store transactional or measured data, and one or more smaller dimensional tables that store attributes about the data.

The analytics teams wants to implement this architecture to understand what songs users are listening to. Therefore, we have used table to describe songs details, another table to describe users behaviors and the third table stores information details about artists. The fourth table : songplays resumes relationship between users, songs and artists. However, The time table is useful for analysis during certain periods like weekend or weekdays, holidays etc. 

- The ETL pipeline consists on :  
        - Extracting the data from the json Repositories in S3 to staging tables on Redshift
        - Transform this data to satisfy the requirements of each of the five tables
        - Insert or load transformed data into analytics tables on Redshift