# Databricks notebook source
#Mount Storage Container to Save Results to
containerName = 'databricks-data'
storageaccountName = 'ajscovid19dashdata'
sas = '?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupitfx&se=2022-05-31T10:08:37Z&st=2022-03-31T02:08:37Z&spr=https&sig=X6unsXrJfhnT4OWl54ssa%2FLdcPny%2F7phGRma69qjMxI%3D'
url = f'wasbs://{containerName}@{storageaccountName}.blob.core.windows.net/'
config = f'fs.azure.sas.{containerName}.{storageaccountName}.blob.core.windows.net'

dbutils.fs.mount(
  source = url,
  mount_point = "/mnt/covid19data",
  extra_configs = {config:sas})
