# Databricks notebook source
#RUN TWINT INIT IF CLUSTER RESTARTED BEFORE ATTACHING THIS NOTEBOOK
#DETACH AND RE-ATTACH IF TWINT INIT WAS RUN AFTER ATTACHING THIS NOTEBOOK


import twint
import nest_asyncio
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import time
import os
import glob
import shutil

nest_asyncio.apply()

# COMMAND ----------

def yesterday():
    yesterday = datetime.now() - timedelta(1)
    return yesterday.strftime('%Y-%m-%d')

def my_display(df):
    display(df[['id', 'date', 'time', 
           'username', 'name', 
           'tweet', 'language', 
           'hashtags',
           'replies_count', 'retweets_count','likes_count',]])

def check_time(time) -> bool:
    try:
        datetime.strptime(time, '%H:%M:%S')
        return True
    except:
        return False
    
def remove_duplicates(df):
    new_df = df.drop_duplicates(subset=['link'], keep='last').reset_index(drop=True)
    return new_df

def count_duplicates(df):
    print(df.duplicated(subset=['link']).sum())

# COMMAND ----------

def clean_and_sort(csv_file):
    df = pd.read_csv(csv_file, lineterminator='\n')
    df = remove_duplicates(df)
    
    #sort by time then date
    sorted_df = df.sort_values(["time"], ascending=False)
    sorted_df = sorted_df.sort_values(["date"], ascending=False, kind="mergesort")
    
    #mask to remove ads
    mask = sorted_df['time'].apply(check_time)
    
    #final_df has ads removed
    final_df = sorted_df[mask]
    
    #write to file
    #fileName = os.path.splitext(csv_file)[0]
    #fileName = fileName + "_preprocessed.csv"
    final_df.to_csv(csv_file, index=False)

def combine_csv_by_country(country, date):
    topic_list = ['ukraine', 'russia']
    extension = 'csv'
    for topic in topic_list:
        header = topic + "_" + country

        expr = header + '*.{}' 
        expr = expr.format(extension)
        
        #Get all fileanmes matching expression
        all_filenames = [i for i in glob.glob(expr.format(extension))] #eg 'matching ukraine_United states*.csv'
        print(all_filenames)
        if not all_filenames:
            return
        #Combine all found csvs into one Df
        combinedDf = pd.concat([pd.read_csv(f, lineterminator='\n') for f in all_filenames])

        #Standardise columns
        combinedDf['near'] = country
        combinedDf['date'] = date
        combinedDf['topic'] = topic

        finalDf = combinedDf[
            ['id', 'date', 'tweet', 'language', 'near', 'topic']
        ]

        combinedDf.to_csv("./" + header + "_" + date + "_tweets.csv", index=False)

        for f in all_filenames:
            os.remove(f)

def combine_csv_by_topic(topic, date):
    extension = 'csv'
    expr = topic + '*.{}' #'ukraine*.csv eg
    
    #Get all fileanmes matching expression
    all_filenames = [i for i in glob.glob(expr.format(extension))] #eg 'matching ukraine*.csv'

    #Combine all found csvs into one Df
    combinedDf = pd.concat([pd.read_csv(f) for f in all_filenames ])
    
    combinedDf = pd.concat([pd.read_csv(f) for f in csv_list])
    combinedDf.to_csv("./" + topic + "_" + date + "_tweets.csv")
    for csv in csv_list:
        os.remove(csv)

def construct_output_filename(topic, country, city, date):
    return ("./" + topic + "_" + 
            country + "_" + 
            city + "_" +
            date +
            "_tweets.csv")
    
def construct_filepath(isDest, filename):
    if isDest:
        return ('/dbfs/FileStore/raw_tweets/' + filename)
    else:
        return ('/databricks/driver/' + filename)
    
def get_twint_config(dest_csv_file_path, resume_file_path, keyword, city, date):
    date_str_arr = date.split('-')
    year = date_str_arr[0]
    month = date_str_arr[1]
    day = date_str_arr[2]
    
    c = twint.Config()
    c.Search = keyword
    c.Hide_output = True
    c.Count = True
    c.Pandas = True
    
    #Calculate until date
    datetime_start_date = datetime.strptime(date, '%Y-%m-%d')
    date_time_end_date = datetime_start_date + timedelta(days=1)
    end_date = datetime.strftime(date_time_end_date, '%Y-%m-%d')
    
    c.Since = date
    c.Until = end_date
    c.Lang = 'en'
    c.Near = city
    c.Store_csv = True
    c.Output = dest_csv_file_path
    c.Limit = 10000000000
    c.Resume = resume_file_path
    
    return c

#Returns Dest file path for reference
def search_by(topic, keyword, country, city, date):
    #Setup
    resume_file_path = "./resume_"+ country + "_" + city + ".txt"
    dest_csv_file_path = construct_output_filename(topic, country, city, date)

    c = get_twint_config(dest_csv_file_path, resume_file_path, keyword, city, date)
    
    start = time.time()

    #Initiate search
    print('Initiating search')
    twint.run.Search(c)
    print('Search done')
    end = time.time()
    elapsed = round((end - start) / 60, 0)
    print('Search for Keyword: ' + keyword + ' for city: ' + city + ' took ' + str(elapsed) + ' minutes')
    
    os.remove(resume_file_path)
    
    return dest_csv_file_path
    

def make_dirs(date):
    ukraine_folder = 'dbfs:/FileStore/raw_tweets/' + date + '/ukraine/'
    russia_folder = 'dbfs:/FileStore/raw_tweets/' + date + '/russia/'
    dbutils.fs.mkdirs(ukraine_folder)
    dbutils.fs.mkdirs(russia_folder)



# COMMAND ----------

ukraine_keywords = ['ukraine',
                   'kiev',
                   'ukranians',
                   'kyiv',
                   'zelensky',
                   'ukranian forces',
                   'ukranian army',
                   'ukranian government']

russian_keywords = ['russia',
                   'russian',
                   'russian forces',
                   'russian army',
                   'putin',
                   'russian government',
                   'russian federation']

# COMMAND ----------

# !rm *.csv
# !rm *.txt
# os.listdir('/databricks/driver')

# COMMAND ----------

world_cities_sparkdf = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/e0325748@u.nus.edu/worldcities.csv")
start_row = 0
world_cities_df = world_cities_sparkdf.toPandas().iloc[start_row: , :] 


# COMMAND ----------

countries = set()

# COMMAND ----------

start = time.time()
last_index = world_cities_df.index[-1]
date = yesterday()

#Loop through all top population cities
while True:
    start_row = 0
    world_cities_df = world_cities_df.iloc[start_row: , :] #top 5035 rows are all cities with population above 100k
    for index, row in world_cities_df.iterrows():
        country = row['country']
        city = row['city_ascii']
        curr_index = (world_cities_df[(world_cities_df['country']  == country) & (world_cities_df['city_ascii'] == city)].index.tolist())[0]
        isLast = False
        hasData = False
        if curr_index == last_index:
            isLast = True
        
        #Track which countries we have seen
        if country not in countries:
            countries.add(country)

        #Set up date parameters to search
        #Search focuses on yesterday 8am - today 8am (technically 1 day since we are gmt +8)
        #We will consider this scrape as 1 days worth of tweets starting from gmt 12am 
        hasData = True

        try:
            for keyword in ukraine_keywords:
                print("Ukraine - Before search - " + city)
                output = search_by('ukraine', keyword, country, city, date)
                print("Ukraine - After search - " + city)

                if os.path.exists(output):
                    clean_and_sort(output)
                    hasData = True

            for keyword in russian_keywords:
                print("Russia - Before search - " + city)
                output = search_by('russia', keyword, country, city, date)
                print("Russia - After search - " + city)
                if os.path.exists(output):
                    clean_and_sort(output)
                    hasData = True

            if hasData and country not in countries:
                countries.add(country)
            
            if isLast:
                break
        
        except twint.token.RefreshTokenException:        
            if curr_index != last_index:
                start_row = curr_index
                continue
            else:
                break
    

end = time.time()
elapsed = round((end - start) / 60, 0)
print('Scrape took: ' + str(elapsed) + ' minutes')

# COMMAND ----------

countriesDf = pd.DataFrame(countries, columns=['countries'])
countriesDf.to_csv('/dbfs/FileStore/countries.csv', index=False)

# COMMAND ----------

countriesDf = pd.DataFrame(countries)
countriesDf.to_csv('/dbfs/FileStore/countries.csv', index=False)

for country in countries:
    combine_csv_by_country(country, date)


# COMMAND ----------

make_dirs(date)

for country in countries:
    topic_list = ['ukraine', 'russia']
    for topic in topic_list:
        folder_name = date + '/' + topic + '/'
        filename = topic + "_" + country + "_" + date + "_tweets.csv"
        src = construct_filepath(False, filename)
        dest = construct_filepath(True, folder_name + filename)
        if os.path.exists(src):
            shutil.move(src, dest)
