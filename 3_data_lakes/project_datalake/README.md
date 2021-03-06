# Project - Big data processing with PySpark
______

# Dataset overview

The datasets used in this project reside in Amazon S3. The dataset is composed by two subdatasets:

- Song Dataset: is a subset of a real dataset from the http://millionsongdataset.com/. Each file is in JSON format and contains metadata about the songs and the artists of each song.

- Log Dataset: contains log files in JSON format generated by the [event simulator](https://github.com/Interana/eventsim).

# Database schema

The script process the Song files and Log files using PySpark and returns 6 tables in parquet file format:

- staging_events (Staging Table for Log Dataset): stores the raw data from the Log Datasets

- staging_songs (Staging Table for Songs Dataset): stores the raw data from de Log Dataset.

- songplays (Fact Table): contains all the registers of user activity, describing which songs have listened by the user.

- users (Dimension Table): contains the data that describe the users;

- songs(Dimension Table): contains the data that describes artists;

- artists (Dimension Table): contains the data about that describe the artist.

- time (Dimension Table): contains the data that describe the time of each activity, and allows to identify activity by different time attributes (hour, day, weekday, month, week)


# Implementation Instructions

Create a dl.cfg file with the amazon AWS credentials.

Create an EMR Cluster on AWS and allows an SSH connection to the master node.

Create an S3 bucket on AWS. (Your S3 bucket needs to be located at the same EMR Region)

Stablish a tunnel connection with the EMR master node in your local machine.

Copy the `etl.py` and and `dl.cfg` to the master node.

Run the `etl.py` using the command `spark-submit etl.py`.

Ps.: You might install the PySpark in you EMR master node using `pip install pyspark`
