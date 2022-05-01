# Sentiment Analysis of Tweets (on the 2022 Russian Invasion of Ukraine)

This project focuses on the analysis of big data that is present on Twitter in hopes of amassing knowledge of the consensus people have towards prominent current affairs, specifically about the ongoing crisis between Ukraine and Russia.
## Cluster Configuration
The Databricks cluster configuration in JSON is
```json
{
    "autoscale": {
        "min_workers": 1,
        "max_workers": 2
    },
    "cluster_name": "twitter_this (clone)",
    "spark_version": "9.1.x-scala2.12",
    "spark_conf": {
        "spark.mongodb.write.connection.uri": "mongodb://cs4225-output:SdSslZkQQxRWmSIlEZaVcNIRU6pBPAwHB6yTmINgsIP4CIJ1j8IEFieCDM6k13vzhiAB0WCjvzVJvLc4I1HcaA==@cs4225-output.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@cs4225-output@",
        "spark.mongodb.read.connection.uri": "mongodb://cs4225-output:SdSslZkQQxRWmSIlEZaVcNIRU6pBPAwHB6yTmINgsIP4CIJ1j8IEFieCDM6k13vzhiAB0WCjvzVJvLc4I1HcaA==@cs4225-output.mongo.cosmos.azure.com:10255/?ssl=true&readPreference=primaryPreferred&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@cs4225-output@",
        "spark.databricks.delta.preview.enabled": "true",
        "spark.kryoserializer.buffer.max": "2000M",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "fs.azure.account.key.sentanaly.dfs.core.windows.net": "{{secrets/sentanaly-scope/sentanaly-key}}"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_DS4_v2",
    "driver_node_type_id": "Standard_DS4_v2",
    "ssh_public_keys": [],
    "custom_tags": {},
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "autotermination_minutes": 120,
    "enable_elastic_disk": true,
    "cluster_source": "UI",
    "init_scripts": [],
    "cluster_id": "0425-162439-tl1vqfd3"
}
```
## Requirements
```
com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.3
org.mongodb.spark:mongo-spark-connector:10.0.0
aiodns
beautifulsoup4
cchardet
dataclasses
elasticsearch
pysocks
pandas>=0.23.0
aiohttp_socks<=0.4.1
schedule
geopy
fake-useragent
googletransx
aiohttp==3.7.0
spark-nlp==3.4.3
git+https://github.com/woluxwolu/twint.git@origin/master#egg=twint
```

## Important Files

1. tweet_scraper.ipynb
This notebook is responsible for initial data generation, and possible day to day scheduled updating of the displayed results. It uses Twint the twint library to scrape tweets. It is meant to be a notebook scheduled to run daily, where it would use yesterday’s date, as a starting point for the scrape. 

   - The scrape should start no earlier than 8am on the following day. For example, if we intend to scrape tweets for the day of 01/05/2022, we have to wait until 8am of 02/05/2022 to scrape. This is due to the fact that Twint likely uses the GMT timezone for interpreting dates, and we are in the GMT +8 timezone, hence if we attempt to scrape before 8am, the supplied parameters to the date configuration would be interpreted as incorrect, and no tweets would be scraped.

   - All scrape outputs are of the format “[ukraine/russia]_[country]_[city]_[date]_tweets.csv”, which would be combined into a standardised “[ukraine/russia]_[country]_[date]_tweets.csv” after the entire scrape is done, and moved to the corresponding folder marked by the path “.../raw_tweets/[date]/[ukraine/russia]” to be processed later using our processing pipeline.

1. Processing Pipeline.ipynb
This notebook is responsible for the main processing pipeline, which consists of pre-processings, sentiment analysis, post-processings as well as interactions with external data storage (e.g. Delta Lake and CosmosDB)

	It consists of 3 parts:
- The main processing pipeline we used for batching processing
- Some small test cases to provide general ideas of each command in the pipeline
- Testing the connection (read or write) of different external storages.


## Frontend and Backend

The frontend code is a Flutter webpage that has been compiled into JavaScript code and the contents of the folder frontend can be deployed directly similar to a built React app.

The backend azure-functions-node requires an Azure subscription to deploy to Azure functions. A detailed tutorial using Mongoose ORM is available at https://docs.microsoft.com/en-us/azure/developer/javascript/tutorial/azure-function-cosmos-db-mongo-api. Our code uses the official Node driver directly.

A sample query can be made via
```bash
curl -X GET http://sentanaly.azurewebsites.net/api/sentimentsoverall -H 'Content-Type: application/json' --verbose
```



