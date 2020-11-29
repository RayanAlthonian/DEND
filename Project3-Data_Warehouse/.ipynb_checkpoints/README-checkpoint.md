# Project 3: Data Warehouse 

### Purpose of This Project

<p style='text-align: justify;'> The project will allow students to build an ETL pipeline that extracts data from a storage system and apply certain transformations that are meant to turn the data into a set of dimensional tables for which the analytics team can do their analysis in a much simpler and efficient way. </p>

### Database Schema 

The following figure illustrates the star schema that has been created in this project.

![Star_Schema](schema.png)

### Files in This Repo

This repo contains the following files: 

1- create_table.py: Creates fact and dimension tables for the star schema in Redshift.

2- etl.py: loads data from S3 into staging tables on Redshift and then processes that data into the analytics tables on Redshift.

3- sql_queries.py: defines SQL statements, which will be imported into the two other files above.

### How to Run The Scripts 

Run the following in the terminal: 

1- python create_tables.py 

2- python ETL.py 

(1) will drop tables and then create new ones while (2) loads data into staging tables, then extract the data from them and store it in a set of dimensional tables that follow the star schema. 