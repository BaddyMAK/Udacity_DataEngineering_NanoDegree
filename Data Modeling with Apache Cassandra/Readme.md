A startup providing music on their website wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. They'd like a data engineer to create an Apache Cassandra database which can create queries on data to answer the questions.

This project aims to create a database for this analysis using a provided template in the format of Jupyter Notebook. Finally, I must test my database by running given queries by the analytics team from startup to create the results.

In addition, I must apply what I've learned on data modeling with Apache Cassandra chapter during this nanodegree. Then, I must complete an ETL pipeline using Python. To complete the project, I used to model the given data by creating tables in Apache Cassandra to run queries. Actually, a part of the ETL pipeline that transfers data from a set of CSV files within a directory were provided to create a streamlined CSV file, to model and insert data into Apache Cassandra tables. The 

The workflow steps needed to be completed in the Jupyter notebook are the next: 
  - Process the event_datafile_new.csv dataset to create a denormalized dataset
  - Model the data tables keeping in mind the queries needed to be run
  - Write the provided queries which are required to model the given data tables for
  - Load the data into tables created in Apache Cassandra and run the previous queries
