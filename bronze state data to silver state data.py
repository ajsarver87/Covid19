# Databricks notebook source
#This will clean the raw pulled data from the CDC website
#https://data.cdc.gov/Case-Surveillance/United-States-COVID-19-Cases-and-Deaths-by-State-o/9mfq-cb36

#import the bronze table as a pyspark dataframe

bronze_00 = spark.table('default.bronze_state_data')

# COMMAND ----------

# We want to keep conf_cases because that is the running total for cases
# To calculate the number of confirmed cases per week, we have to do new_case - pnew_case
# if pnew_case is blank, should be zero (not every state reports this)
bronze_01 = bronze_00.withColumn('cnew_case', bronze_00.new_case - bronze_00.pnew_case)

#same with deaths
bronze_01 = bronze_01.withColumn('cnew_death', bronze_01.new_death - bronze_01.pnew_death)

# COMMAND ----------

#looking at the data, not every state reports probable cases/deaths
#so we will stick with confirmed numbers
bronze_02 = bronze_01.select('submission_date','state','conf_cases','cnew_case','conf_death','cnew_death','created_at')
