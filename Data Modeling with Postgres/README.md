1- Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.

Sparkify, is a startup that has a large amount of data stored in two directories of JSON. First directory is JSON logs on user activity on the app as well as a directory with JSON metadata on the songs in their app. The collected data contains fields that describes songs and user activity on the new streaming app of Sparkify. 

The problem is that due to the poor storage architecture adopted by the Startup, the analytics teams has difficulties in querying the data and so on in understanding what songs users are listening to.

this project aims to create a Postgres database with tables and ETL pipeline to optimize queries on song play analysis. As a first step, I had created the fact and dimension tables for the star schema. Second, I had inserted records in the created tables following a written ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL

2 - How to run python scripts :
- First you run the python script create_tables.py which calls the two query lists create_table_queries, drop_table_queries from ql_queries. 
- Second run the script etl.py which perform the same tasks as in jupyter notebook etl.ipynb but on the whole data sets
- The jupyter notebook test.ipynb is to test the provided scripts conormity with respect to the project target


3- An explanation of the files in the repository :

- We have two datasets stored in two different directories under the data folder. They are Song Dataset and Log Dataset
- The create_tables.py : calls list of sql queries defined in sql_queries.py
- Queries in sql_queries.py have three main role: The first role is creating five tables named songplays, users, songs, artists and time. Second role,  inserting records in tables. The last role is deleting tables at the end.
- etl.py has almost the same role as the etl.ipynb jupyter notebook. The only difference is that etl.py is performing an etl process over the whole given dataset.
- test.ipynb is to test if the tables were created successfully, if records have been already inserted etc.


4- State and justify your database schema design and ETL pipeline.

- we have chosen postgres as database because our data is structured means each field in each json file has only one value so it is easy to convert those json files into tables. 
- The star schema that uses a single large fact table (songplays) to store transactional or measured data, and one or more smaller dimensional tables that store attributes about the data.
- The ETL pipeline consists on :  
        - Extracting the data from the json Repositories 
        - Transform this data to satisfy the requirements of each of the five tables
        - Insert or Load the records into the Tables 
- The analytics teams wants to implement this architecture to understand what songs users are listening to. Therefore, we have used table to describe songs details, another table to describe users behaviors and the third table stores information details about artists. The fourth table : songplays resumes relationship between users, songs and artists. 
- The time table is useful for analysis during certain periods like weekend or weekdays, holidays etc. 



