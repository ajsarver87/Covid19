# Databricks notebook source
#pull in basic libraries
#use basic request library to make API calls
import requests

# COMMAND ----------

# pull in the raw Covid 19 State Data from CDC
# Since I can only pull in 1000 rows at a time I will loop through all the data in chunks until I get an empty chunk
#Data Set URL = https://data.cdc.gov/Case-Surveillance/United-States-COVID-19-Cases-and-Deaths-by-State-o/9mfq-cb36

api_endpoint = 'https://data.cdc.gov/resource/9mfq-cb36.json'
api_token = 'qnEZ0rOS7wZ2E2lZJarNWTNRE'  

file_path = r'/mnt/covid19data/raw_state_data/'
offset = 0
chunk = 1

while chunk > 0:
    if offset > 0:
        url_string = api_endpoint+f'?$offset={offset}'
    else:
        url_string = api_endpoint
        
    print('Sending Get Request to ' + url_string)
    r = requests.get(url_string, headers={'X-App-token':api_token})
    
    chunk = len(r.json())
                                          
    if chunk > 0:
        dbutils.fs.put(file_path + f'state_raw_data_{offset}.json', str(r.json()), True)
        
        if offset == 0:
            results_df = spark.read.json(file_path + f'state_raw_data_{offset}.json')
        else:
            results_df = results_df.union(spark.read.json(file_path + f'state_raw_data_{offset}.json'))

    offset += 1000

# COMMAND ----------

# you can check the CDC website to verify that the number of rows is the same
print(results_df.count())

# COMMAND ----------

results_df.write.saveAsTable("Bronze_State_Data")
