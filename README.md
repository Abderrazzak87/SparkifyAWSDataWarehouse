# Sparkify AWS Data Warehouse

`Sparkify` is a music streaming startup. It has grown their user and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project aims to build an ETL pipeline that extracts their data from `Amazon Web Services S3`, stages them in `Redshift`, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Data Sources

The data sources are two datasets that reside in S3 :

*  Song data: ```s3://udacity-dend/song_data```
*  Log data: ```s3://udacity-dend/log_data```
*  Log data json path: ```s3://udacity-dend/log_json_path.json```

## Modelisation

Using the song and log datasets a strat schema madel was creacted, with :

* **Fact table** : `songplays` table
* **Dimensional tables** : `users, songs, artists and time`

## Creaction of the Redshift Cluster

To creat the Amazon Redshift database the methode infrastructure as code was chosen. 
The method and the code are described in the jupyter notebook `Infrastructure as Code.ipynb`.

## ETL Process

1. First the staging songs table, the staging events table, the fact table and the dimensional tables are created in the Amazon Redshift database.
2. The staging table are loaded from S3 using COPY FROM command
```
staging_events_copy = ("""
    copy staging_events from {}
    iam_role {} 
    format as json {}
""").format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))
```
3. The fact table `songplays` is loaded by joining the songs and events staging tables and selecting all the event data associated with song plays.
4. The dimensional table `users` is loaded from the staging events table using `SELECT DISTINCT` users attributes.
5. The dimensional table `songs` is loaded from the staging songs table using `SELECT DISTINCT` songs attributes.
6. The dimensional table `artists` is loaded from the staging songs table using `SELECT DISTINCT` artists attributes.
7. The dimensional table `time` is loaded from the `start_time` column of the staging songs table using `SELECT DISTINCT`.


## How to run in the console: 

1. Create an Amazon Redshift Cluster by following the steps described in the `Infrastructure as Code.ipynb` file.

2. Configure `dwh.cfg` file with your credentials, your Redshift cluster address, your IAM ROLE and S3 data sources roots.

3. Run the `create_tables.ipynb` to create staging tables and data warehouse tables.

4. Run the `etl.ipynb` to load staging tables and data warehouse tables.

## Authors

* Abderrazzak BENABDALLAH

## License

The contents of this repository are covered under the [MIT License](LICENSE).
