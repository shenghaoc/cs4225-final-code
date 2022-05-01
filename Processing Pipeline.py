# Databricks notebook source
from datetime import datetime
from datetime import timedelta
import csv


def yesterday():
    yesterday = datetime.now() - timedelta(1)
    return yesterday.strftime('%Y-%m-%d')

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

# store the raw tweets data on dbfs to delta lake

# folder_path = "dbfs:/FileStore/shared_uploads/e0325748@u.nus.edu/russia_processed_fix"

folder_path = "dbfs:/FileStore/raw_tweets/" + yesterday() + "/russia" 

files = dbutils.fs.ls(folder_path)

count1 = 0

for file in files:
    #if file.name[0] == 'r':
    russia_path = file.path
    ukraine_path = re.sub(r"russia", "ukraine", file.path)

    keywords = file.name.split("_")

    country = keywords[0]
    date = keywords[1]
    
    if date == "2022-03-03": #May want to remove this
        continue
    
    count1 += 1
    
    raw_tweets_russia = spark.read.format("csv").option("header", "true").load(russia_path)
    
    # dir = re.sub(r"dbfs:", "/dbfs", ukraine_path)
#     if os.path.exists(dir):
    raw_tweets_ukraine = spark.read.format("csv").option("header", "true").load(ukraine_path)
    raw_tweets = raw_tweets_russia.union(raw_tweets_ukraine)
    
    save_path = "abfss://sentanaly@sentanaly.dfs.core.windows.net/raw_tweets_" + country + "_" + date
    
    raw_tweets.write.format("delta").mode("overwrite").save(save_path)

# COMMAND ----------

#This one is the improvement
folder_path_russia = "/dbfs/FileStore/raw_tweets/" + yesterday() + "/russia/"
folder_path_ukraine = "/dbfs/FileStore/raw_tweets/" + yesterday() + "/ukraine/"

with open('/dbfs/FileStore/countries.csv', newline='') as f:
    reader = csv.reader(f)
    countries = list(reader)

count1 = 0

for country in countries:
    #if file.name[0] == 'r':
    ukraine_file = 'ukraine_' + country + '_' + yesterday() + '_tweets.csv' 
    russia_file = 'russia_' + country + '_' + yesterday() + '_tweets.csv' 
    
    russia_path = folder_path_russia + russia_file
    ukraine_path = folder_path_ukraine + ukraine_file

    keywords = file.name.split("_")

    country = keywords[0]
    date = keywords[1]
    
    if date == "2022-03-03": #May want to remove this
        continue
    
    count1 += 1
    
    raw_tweets_russia = spark.read.format("csv").option("header", "true").load(russia_path)
    
    # dir = re.sub(r"dbfs:", "/dbfs", ukraine_path)
    # if os.path.exists(dir):
    raw_tweets_ukraine = spark.read.format("csv").option("header", "true").load(ukraine_path)
    raw_tweets = raw_tweets_russia.union(raw_tweets_ukraine)
    
    save_path = "abfss://sentanaly@sentanaly.dfs.core.windows.net/raw_tweets_" + country + "_" + date
    
    raw_tweets.write.format("delta").mode("overwrite").save(save_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. The sentimentment analysis processing pipeline

# COMMAND ----------

# import libraries

import re
from pyspark.sql.types import StringType, DateType, LongType, FloatType, IntegerType
from pyspark.sql.functions import *

#import pandas as pd
import numpy as np
import json
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

import os
#CHANGES BELOW
from datetime import datetime
from datetime import timedelta
import csv


def yesterday():
    yesterday = datetime.now() - timedelta(1)
    return yesterday.strftime('%Y-%m-%d')

# COMMAND ----------

# external database setup (using norimal JDBC)

jdbcHostname = "cs4225.database.windows.net"
jdbcDatabase = "CS4225"
jdbcPort = 1433

username = dbutils.secrets.get("4225Scope", "sqlusername")
password = dbutils.secrets.get("4225Scope", "sqlpassword")

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)

# COMMAND ----------

# external database setup (using spark connector)

server_name = "jdbc:sqlserver://cs4225.database.windows.net"
database_name = "cs4225"
url = server_name + ";" + "databaseName=" + database_name + ";"

username = dbutils.secrets.get("4225Scope", "sqlusername")
password = dbutils.secrets.get("4225Scope", "sqlpassword")

pipeline = PretrainedPipeline("analyze_sentimentdl_use_twitter", lang = "en") 

# COMMAND ----------

def tweets_cleaning(text):
    
    # Remove URLs
    resulted_text = re.sub('http://\S+|https://\S+', '', str(text), flags=re.MULTILINE)
    
    # Remove emoticons
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", re.UNICODE)
    resulted_text = re.sub(emoj, '', resulted_text)
    
    # Remove @ and the usernames followed
    resulted_text = re.sub('@\S+', '', resulted_text)
    
    # Remove &amp
    resulted_text = re.sub('&amp\S+', '', resulted_text)
    
    # Remove punctuations & numbers
    # Remove consecutive white space
    resulted_text = re.sub('\W+',' ', resulted_text)
    
    # Lowering all text
    resulted_text = resulted_text.lower()
    
    return resulted_text

cleaning = udf(tweets_cleaning, StringType())

# COMMAND ----------

# Topic grouping
ukraine_keywords = ['ukraine',
                   'kiev',
                   'ukranian',
                   'ukrainian'
                   'kyiv',
                   'zelensky']

russian_keywords = ['russia',
                   'putin']

# 0 -- general; 1 -- Ukraine-specific; 2 -- Russian-specific
def topic_group(text):
    U = any(kw_u in text for kw_u in ukraine_keywords)
    R = any(kw_r in text for kw_r in russian_keywords)
    if U:
        if R:
            return 0
        else:
            return 1
    else:
        if R:
            return 2
        else:
            return 0
        
topic_grouping = udf(topic_group, IntegerType())    

# COMMAND ----------

#folder_path = "dbfs:/FileStore/shared_uploads/e0325748@u.nus.edu/russia_processed_fix"
folder_path = "dbfs:/FileStore/raw_tweets/" + yesterday() + "/russia" 


files = dbutils.fs.ls(folder_path)

count1 = 0

for file in files:
    #if file.name[0] == 'r':
    russia_path = file.path
    ukraine_path = re.sub(r"russia", "ukraine", file.path)

    keywords = file.name.split("_")

    country = keywords[0]
    date = keywords[1]
    
    if date == "2022-03-03": #consider removing
        continue
    
    count1 += 1
    
    raw_tweets_russia = spark.read.format("csv").option("header", "true").load(russia_path)
    
    # dir = re.sub(r"dbfs:", "/dbfs", ukraine_path)
#     if os.path.exists(dir):
    raw_tweets_ukraine = spark.read.format("csv").option("header", "true").load(ukraine_path)
    raw_tweets = raw_tweets_russia.union(raw_tweets_ukraine)
#     else:
#         raw_tweets = raw_tweets_russia

    # print(raw_tweets_russia.count(),raw_tweets_ukraine.count(), raw_tweets.count())

    # Keep only useful columns
    raw_tweets = raw_tweets.select('date', 'tweet', 'language', 'near')

    # filter based on language (english only)
    # And drop unmeaningful locations
    raw_tweets = raw_tweets.filter((col('language') == 'en') & (col('near').rlike("^(?!False)[A-Z].*")))
    cleaned_tweets = raw_tweets.select(to_date(raw_tweets['date'], 'yyyy-MM-dd').alias('date'), cleaning('tweet').alias('text'), col('near').alias('location'))

    cleaned_tweets = cleaned_tweets.filter(size(split(col('tweet'), ' ')) > 2)

    cleaned_tweets = cleaned_tweets.withColumn("topic", topic_grouping('text'))

    # sentiment analysis
    results = pipeline.transform(cleaned_tweets)
    final_results = results.select('location', 'date', 'topic', F.explode(results.sentiment.metadata)).select('location', 'date', 'topic', F.map_values("col")[1].alias('sentiment'))

    final_results.cache()

    # 3 sentiments for different topics
    sentiments_pcpd_overall = final_results.groupBy("location", "date").agg(avg('sentiment').alias('sentiment'), count(col('location')).alias('count'))

    sentiments_pcpd_ukraine = final_results.filter(col("topic") == 1).groupBy("location", "date").agg(avg('sentiment').alias('sentiment'), count(col('location')).alias('count'))

    sentiments_pcpd_russia = final_results.filter(col("topic") == 2).groupBy("location", "date").agg(avg('sentiment').alias('sentiment'), count(col('location')).alias('count'))

    # 3 sentiments for different topics
    sentiments_pcpd_overall = final_results.agg(avg('sentiment').alias('sentiment'), count(col('sentiment')).alias('count'))
    sentiments_pcpd_overall = sentiments_pcpd_overall.withColumn('location', lit(country)).withColumn('date', lit(date))

    if count1 == 1:
        sentiments_pcpd_overall.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("database", "sentanaly") \
            .option("collection", "sentiments_overall") \
            .save()
        
        sentiments_pcpd_ukraine.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("database", "sentanaly") \
            .option("collection", "sentiments_ukraine") \
            .save()

        sentiments_pcpd_russia.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("database", "sentanaly") \
            .option("collection", "sentiments_russia") \
            .save()
    else:
        sentiments_pcpd_overall.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "sentanaly") \
            .option("collection", "sentiments_overall") \
            .save()

        sentiments_pcpd_ukraine.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "sentanaly") \
            .option("collection", "sentiments_ukraine") \
            .save()

        sentiments_pcpd_russia.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "sentanaly") \
            .option("collection", "sentiments_russia") \
            .save()
        
#         break
#     display(sentiments_pcpd_overall)         
#     display(sentiments_pcpd_ukraine)
#     display(sentiments_pcpd_russia)



# COMMAND ----------

folder_path = "dbfs:/FileStore/shared_uploads/e0325748@u.nus.edu/russia_processed_fix"

files = dbutils.fs.ls(folder_path)

count1 = 0

for file in files:
    #if file.name[0] == 'r':
    russia_path = file.path
    ukraine_path = re.sub(r"russia", "ukraine", file.path)

    keywords = file.name.split("_")

    country = keywords[0]
    date = keywords[1]
    if date == "2022-03-03":
        print(russia_path)
        continue
    
    count1 += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Following are some small test cases

# COMMAND ----------

raw_tweets = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/raw/russia_Argentina_2022-04-24_tweets.csv")
display(raw_tweets)
raw_tweets.count()

# COMMAND ----------

display(raw_tweets.groupby('language').count())

# COMMAND ----------

#raw_tweets = raw_tweets.dropna()

# Keep only useful columns
raw_tweets = raw_tweets.select('date', 'tweet', 'language', 'near')

# filter based on language (english only)
# And drop unmeaningful locations
raw_tweets = raw_tweets.filter((col('language') == 'en') & (col('near').rlike("^(?!False)[A-Z].*")))

raw_tweets.printSchema()
raw_tweets.count()

# COMMAND ----------

def tweets_cleaning(text):
    
    # Remove URLs
    resulted_text = re.sub('http://\S+|https://\S+', '', str(text), flags=re.MULTILINE)
    
    # Remove emoticons
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", re.UNICODE)
    resulted_text = re.sub(emoj, '', resulted_text)
    
    # Remove @ and the usernames followed
    resulted_text = re.sub('@\S+', '', resulted_text)
    
    # Remove &amp
    resulted_text = re.sub('&amp\S+', '', resulted_text)
    
    # Remove punctuations & numbers
    # Remove consecutive white space
    resulted_text = re.sub('\W+',' ', resulted_text)
    
    # Lowering all text
    resulted_text = resulted_text.lower()
    
    return resulted_text

cleaning = udf(tweets_cleaning, StringType())
# cleaned_tweets = raw_tweets.select(raw_tweets['date'].cast(DateType()), cleaning('tweet').alias('text'), 'near')
cleaned_tweets = raw_tweets.select(to_date(raw_tweets['date'], 'yyyy-MM-dd').alias('date'), cleaning('tweet').alias('text'), col('near').alias('location'))

cleaned_tweets = cleaned_tweets.filter(size(split(col('tweet'), ' ')) > 2) 
display(cleaned_tweets)

# COMMAND ----------

# Topic grouping
ukraine_keywords = ['ukraine',
                   'kiev',
                   'ukranian',
                   'ukrainian'
                   'kyiv',
                   'zelensky']

russian_keywords = ['russia',
                   'putin']
text = "ukrainefa"
any(kw_u in text for kw_u in ukraine_keywords)

# 0 -- general; 1 -- Ukraine-specific; 2 -- Russian-specific
def topic_group(text):
    U = any(kw_u in text for kw_u in ukraine_keywords)
    R = any(kw_r in text for kw_r in russian_keywords)
    if U:
        if R:
            return 0
        else:
            return 1
    else:
        if R:
            return 2
        else:
            return 0
        
topic_grouping = udf(topic_group, IntegerType())        
cleaned_tweets = cleaned_tweets.withColumn("topic", topic_grouping('text'))
display(cleaned_tweets)

# COMMAND ----------

# sentiment analysis
pipeline = PretrainedPipeline("analyze_sentimentdl_use_twitter", lang = "en") 
results = pipeline.transform(cleaned_tweets)
final_results = results.select('location', 'date', 'topic', F.explode(results.sentiment.metadata)).select('location', 'date', 'topic', F.map_values("col")[1].alias('sentiment'))
# sentiment_result - date - country
# cleaned_tweets = cleaned_tweets.withColumn('sentiment', (rand() * 10 - 5).cast(FloatType()))
# display(cleaned_tweets)
# cleaned_tweets.count()

final_results.cache()

display(final_results)

# COMMAND ----------

sentiments_pcpd_overall = final_results.groupBy("location", "date").agg(avg('sentiment').alias('sentiment'), count(col('location')).alias('count'))

sentiments_pcpd_ukraine = final_results.filter(col("topic") == 1).groupBy("location", "date").agg(avg('sentiment').alias('sentiment'), count(col('location')).alias('count'))

sentiments_pcpd_russia = final_results.filter(col("topic") == 2).groupBy("location", "date").agg(avg('sentiment').alias('sentiment'), count(col('location')).alias('count'))

display(sentiments_pcpd_overall)
display(sentiments_pcpd_ukraine)
display(sentiments_pcpd_russia)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Testing of different external storage

# COMMAND ----------

# Post Processing
# sentiment per country per day


# save to SQL table using JDBC
# sentiments_pcpd.write.mode("append").jdbc(jdbcUrl, "sentiments_date_country")
# sentiments_pcpd.write.mode("append").saveAsTable("twitter.sentiment_country_date")


# save to SQL table using spark connector
sentiments_pcpd_overall.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", "sentiments_overall") \
    .option("user", username) \
    .option("password", password) \
    .save()

sentiments_pcpd_ukraine.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", "sentiments_ukraine") \
    .option("user", username) \
    .option("password", password) \
    .save()

sentiments_pcpd_russia.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", "sentiments_russia") \
    .option("user", username) \
    .option("password", password) \
    .save()

# COMMAND ----------

# Save to COSMOS DB
sentiments_pcpd_overall.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("database", "sentanaly") \
    .option("collection", "sentiments_overall") \
    .save()

sentiments_pcpd_ukraine.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("database", "sentanaly") \
    .option("collection", "sentiments_ukraine") \
    .save()

sentiments_pcpd_russia.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("database", "sentanaly") \
    .option("collection", "sentiments_russia") \
    .save()

# COMMAND ----------

jdbcDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", "sentiments_overall") \
        .option("user", username) \
        .option("password", password).load()

display(jdbcDF)

# COMMAND ----------

# generate word cloud using cleaned tweets

from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt
stopwords_set = set(STOPWORDS)
wordcloud = WordCloud(background_color='white',
                     stopwords = stopwords_set,
                      max_words = 150,
                      max_font_size = 40,
                      random_state=42
                     ).generate(cleaned_tweets.agg(concat_ws(" ", collect_list(cleaned_tweets['tweet']))).head()[0])
print(wordcloud)
plt.imshow(wordcloud)
plt.axis('off')
plt.show()
