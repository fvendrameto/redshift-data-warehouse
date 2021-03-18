# Postgres Data Modeling Project

## Summary

This repository implements an ETL procedure to create analytical tables in a cloud hoster data warehouse, using AWS Redshift. The data is obtained from two sources: a songs database and a user activity logs database. Both datasets are distributed in JSON files and hosted on a AWS S3 bucket.

The data was modeled in a Star Schema, with four dimension tables and a fact table:

- `artists`: Dimension table with data from various artists, obtained through the songs dataset.
- `songs`: Dimension table with data from various songs, also obtained from the songs dataset.
- `users`: Dimension table with app user data, obtained from the logs dataset.
- `time`: Dimension table with timestamps references, also obtained from the logs dataset.
- `songplays`: Fact table, with references to all dimensions and additional data obtained from the logs dataset, such as the user session id, the location of this session and the application user agent.

## Project organization

This project is implemented in Python and has several modules, which are described ahead:

- `config.py`: loads a file `dwh.cfg` that variables with database information, such as the host, user and password, AWS secret keys and roles, etc.
- `create_tables.py`: resets the database, recreating all tables. On the first execution you must run this script before `etl.py` in order to create the tables.
- `etl.py`: executes several queries to load the JSON datasets into staging tables and insert the data from these tables to the dimensions and fact tables.
- `sql_queries.py`: has queries to create, insert and drop tables.
- `setup_cluster.py`: creates a Redshift cluster with the configurations described on `dwh.cfg`. This is an alternative to creating the cluster on AWS console.
- `teardown_cluster.py`: deletes the cluster from AWS. Be aware that this will removea ll data permanently.
- `dwh.cfg`: has all variables related to database configuration and AWS.

## Execution

This project is meant to run on Udacity's environment, with access to S3 buckets which may be private. Besides that, all code can be executed outside this environment with a proper `dwh.cfg` file, which isn't provided publicly because it has AWS secrets.

**IMPORTANT**: if you created your cluster manually you must set the `host` variable on `dwh.cfg` to your cluster's host. When using the scripts provided here, this variable is set automatically.

There are two executable scripts in this project and they're pretty straigthforward.

```bash
python create_tables.py
python etl.py
```

On a fresh database, the tables won't exist so be sure to run `create_tables.py` before `etl.py` or you will encounter errors. In this project, `etl.py` is meant to populate a fresh database and therefore, if run more than once it will generate duplicates. Redshift doesn't enforce primary key constraints, so this is possible even if they are defined.
