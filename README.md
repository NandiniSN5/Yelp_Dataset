# Spark Cluster with Docker & docker-compose(2021 ver.)

# General

The Docker compose will create the following containers:

container|Exposed ports
---|---
spark-master|9090 7077
spark-worker-1|9091
spark-worker-2|9092


# Installation

The following steps will make you run your spark cluster's containers.

## Pre requisites

* Docker installed

* Docker compose  installed

## Build the image


```sh
docker build -t cluster-apache-spark:3.0.2 .
```

## Run the docker-compose

The final step to create your test cluster will be to run the compose file:

```sh
docker-compose up -d
```

## Validate your cluster

Just validate your cluster accesing the spark UI on each worker & master URL.

### Spark Master

http://localhost:9090/

![alt text](docs/spark-master.png "Spark master UI")

### Spark Worker 1

http://localhost:9091/

![alt text](docs/spark-worker-1.png "Spark worker 1 UI")

### Spark Worker 2

http://localhost:9092/

![alt text](docs/spark-worker-2.png "Spark worker 2 UI")


# Resource Allocation 

This cluster is shipped with three workers and one spark master, each of these has a particular set of resource allocation(basically RAM & cpu cores allocation).

* The default CPU cores allocation for each spark worker is 1 core.

* The default RAM for each spark-worker is 1024 MB.

* The default RAM allocation for spark executors is 256mb.

* The default RAM allocation for spark driver is 128mb

* If you wish to modify this allocations just edit the env/spark-worker.sh file.

# Binded Volumes

To make app running easier I've shipped two volume mounts described in the following chart:

Host Mount|Container Mount|Purposse
---|---|---
Raw        |/opt/spark-Raw|         Used to make available your app's Raw data file on all workers & master
Clean      |/opt/spark-Clean|       Used to store clean data file after cleaning and transforming workers & master
Aggregate  |/opt/spark-Aggregate|   Used to store aggregated data file after aggregation workers & master
Apps       |/opt/spark-Apps     |   Used to store python file for executing the clean and aggregated files

This is basically a dummy DFS created from docker Volumes...(maybe not...)

# Run Sample applications


## Yelp Dataset JSON Cleaning & Transforming  [Pyspark] - Yelp_dataset_pyspark.py
Note : As the data json files are huge and cannot be loaded into github account.
Kindly download and upload the data files into /opt/spark-Raw inside the container to run the applications. 

This programs just loads archived data from different json files such as business.json,review.json,user.json,checkin.json,tip.json 
and apply basic checks for duplicate records, cleaning, Normalization of data and replacing null values with NA or appropraite missing data value using pyspark sql .
The clean results are finally stored in spark-Clean folder.

To submit the app connect to one of the workers or the master and execute:

```sh
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--total-executor-cores 1 \
--class mta.processing.MTAStatisticsApp \
--driver-memory 1G \
--executor-memory 1G \
/opt/spark-apps/Yelp_dataset_pyspark.py \
/opt/spark-Raw/yelp_academic_dataset_business.json \
/opt/spark-Raw/yelp_academic_dataset_checkin.json  \
/opt/spark-Raw/yelp_academic_dataset_review.json  \
/opt/spark-Raw/yelp_academic_dataset_user.json  \
/opt/spark-Raw/yelp_academic_dataset_tip.json  \
```
Note :Please submit the files in same order as given in the example and you can also specify total executors, driver memory and executor memory
Once the program is submitted and completed ,we can see the program results in /opt/spark-Clean/ folder with folder names
Cleaned_BusinessUnit.json -- For Business Json files 
Cleaned_CheckInUnit.json  -- For CheckIn  Json Files
Cleaned_ReviewUnit.json   -- For Review   Json files 
Cleaned_UserUnit.json     -- For User     Json Files
Cleaned_TipUnit.json      -- For Tips     Json Files

## Yelp Dataset JSON Aggreation [Pyspark] - Yelp_Aggreagte_Dataset_Pyspark.py
Note : As the data json files are huge and cannot be loaded into github account.
Kindly download and upload the data files into root directory "/" of docker inside the container to run the applications. 

This program takes the and make some aggregations on it and store the files back in /opt/spark-Aggregate/
Three aggreated files are created.

To submit the app connect to one of the workers or the master and execute:

```sh
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
/opt/spark-apps/Yelp_Aggreagte_Dataset_Pyspark.py
```
Note : In this program, we are not passing the path of the json files, as it is hardcoded to fetch files from root directory.
Kindly add the json files in root directory of docker container.


Aggregation
---|---------
-- Business.Json, Tip.json and CheckIn.json,Review.json and User.json files are loaded from respective 
   files and cleaned and created respective BusinessUnit,TPUnit,CheckInUnit,ReviewUnit,UserUnit table on them.
   
   Broadcasted BusinessUnit,TPUnit,CheckInUnit table as they are smaller dataset to reduce huge shuffle that occur in 
   UserUnit and ReviewUnit, as these datasets are too huge.
   
Template_df_BuRvTP.json:
------------------------
Business name with its overallrate, all the users who has given tips based on their checkin date in respective business and 
their ratings are given.

Template_df_BuTpCnt.json:
---------------------------
Business name with its overallrate, count of checkin based on week and year and user rates for the same are given.

Template_df_BuRvCnt.json:
---------------------------
Business name with its overallrate, count of checkin based on week and year and review rates for the same are given.

Possible Problems :
start-spark.sh - while running this docker file , it might give error.
Kindly replace the CRLF with LF.



