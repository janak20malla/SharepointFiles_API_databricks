# Databricks notebook source
import pandas as pd
import datetime
import numpy as np
import requests
from collections import Counter
import sys
import os
import base64
import time
import copy
import json
dbutils.library.installPyPI('adal')
import adal
import urllib
from collections import defaultdict
import csv

# COMMAND ----------

def authenticate_client_key():
    authority_host_uri = 'https://login.microsoftonline.com'
    tenant = 'tenantID'
    authority_uri = authority_host_uri + '/' + tenant
    resource_uri = 'https://graph.microsoft.com/'
    client_id = 'client_id'
    client_secret = 'client_secret'

    context = adal.AuthenticationContext(authority_uri, api_version=None)
    mgmt_token = context.acquire_token_with_client_credentials(resource_uri, client_id, client_secret)
    return mgmt_token

# COMMAND ----------

token = authenticate_client_key()
hvalue = "Bearer " + token['accessToken']
headers = {}
headers['Authorization'] = hvalue

# COMMAND ----------

# Creating widgets for leveraging parameters
dbutils.widgets.text("input1", "","")

# Collect parameter inputs from Data Factory
NameId = dbutils.widgets.get("input1")

# COMMAND ----------

# NameId = ["lookup_pricing_source.xlsx->01DKXIJUWPLZCKEONPYJB2VGHQ2LD3LWSN","RMBS - post.xlsx->01DKXIJUV7XVIVBUPYNRALFRORLPIEME3Q","RMBS - day2.xlsx->01DKXIJUU252M73MQ7YZDZPHYIYENYADQV","RMBS - day1.xlsx->01DKXIJUVBCVK46LST7ZHZLL32JEGXQ7AN","RMBS - pre.xlsx->01DKXIJUWA254RNK67QBD2KWO3YMSV3Y6X"]

# COMMAND ----------

#  NameId = ["PriceDay1.csv->01DKXIJUSTGCUXQFKWPVDJJ3ZC2HOJPX7J","PriceDay2.csv->01DKXIJURNEZ4GCEN5IVA25S6DK6SWEF2P","lookup_pricing_source.xlsx->01DKXIJUWPLZCKEONPYJB2VGHQ2LD3LWSN","PricePost.csv->01DKXIJUSDCG2PVGCJLVBK5SODBD7RUAUB","PricePre.csv->01DKXIJUXVQJJAEUKKEVBZ5N527DBJHBZV","RMBS - post.xlsx->01DKXIJUV7XVIVBUPYNRALFRORLPIEME3Q","RMBS - day2.xlsx->01DKXIJUU252M73MQ7YZDZPHYIYENYADQV","RMBS - day1.xlsx->01DKXIJUVBCVK46LST7ZHZLL32JEGXQ7AN","RMBS - pre.xlsx->01DKXIJUWA254RNK67QBD2KWO3YMSV3Y6X"]

# COMMAND ----------

# NameId = ["lookup_pricing_source.xlsx->01DKXIJUWPLZCKEONPYJB2VGHQ2LD3LWSN","RMBS - post.xlsx->01DKXIJUV7XVIVBUPYNRALFRORLPIEME3Q","RMBS - day2.xlsx->01DKXIJUU252M73MQ7YZDZPHYIYENYADQV","RMBS - day1.xlsx->01DKXIJUVBCVK46LST7ZHZLL32JEGXQ7AN","RMBS - pre.xlsx->01DKXIJUWA254RNK67QBD2KWO3YMSV3Y6X"]

# COMMAND ----------

NameId = json.loads(NameId)

# COMMAND ----------

type(NameId)

# COMMAND ----------

#Dictionary of inputs (file name and id number) together.
dict1 = dict(s.split('->') for s in NameId)

# COMMAND ----------

dict1

# COMMAND ----------

# All xlsx files 
lookupFileDict = dict()
otherFileDict = dict()
csvFileDict = dict()

for element in dict1:
  if (element.startswith('lookup')):
    lookupFileDict.__setitem__(element,dict1[element])

  elif(element.startswith('RMBS')):
    otherFileDict.__setitem__(element,dict1[element])
  else:
    csvFileDict.__setitem__(element,dict1[element])


# COMMAND ----------

                            #For xlsx and csv files
# xlsxDict = dict()
# csvDict = dict()

# for element in dict1:
#   if (element.endswith('.xlsx')):
#     xlsxDict.__setitem__(element,dict1[element])

#   else:
#     csvDict.__setitem__(element,dict1[element])


# COMMAND ----------

# xlsx files excluding the lookup file.
for i in otherFileDict:
  print(i,otherFileDict[i])

# COMMAND ----------

# xlsx lookup file.
for i in lookupFileDict:
  print(i,lookupFileDict[i])


# COMMAND ----------

# csv files excluding the lookup file. 
for i in csvFileDict:
  print(i,csvFileDict[i])

# COMMAND ----------

######################################################################## For Lookup file ###################################################################################

# COMMAND ----------

if (len(lookupFileDict)> 0):
  url = "https://graph.microsoft.com/v1.0/sites/sharepointlocation/drive/items/{}/workbook/worksheets/".format(list(lookupFileDict.values())[0])
  r = requests.get(url, headers=headers)
  sheetNames = []
  for value in r.json()['value']:
    sheetNames.append(value['name'])
  for i in range(len(sheetNames)):
    url = "https://graph.microsoft.com/v1.0/sites/sharepointlocation/drive/items/{}/workbook/worksheets/{}/usedRange/".format(list(lookupFileDict.values())[0],sheetNames[i])
    r = requests.get(url, headers=headers)
    if sheetNames[i] == "PricingDays":
      PricingDays = pd.DataFrame(r.json()['text'][1:], columns=r.json()['text'][0])
      PricingDays.columns = map(str, PricingDays.columns.str.replace(' ', ''))
      PricingDays = PricingDays.replace({ "": np.nan})
      PricingDays.dropna(axis=0, how='all', thresh=None, subset=None, inplace=True)
      PricingDays = PricingDays.replace(np.nan, '', regex=True)
      PricingDays = PricingDays.drop_duplicates()
    elif sheetNames[i] == "SycodeToExclude":
      SycodeToExclude = pd.DataFrame(r.json()['values'][1:], columns=r.json()['values'][0])
      SycodeToExclude.columns = map(str, SycodeToExclude.columns.str.replace(' ', ''))
      SycodeToExclude = SycodeToExclude.replace({ "": np.nan})
      SycodeToExclude.dropna(axis=0, how='all', thresh=None, subset=None, inplace=True)
      SycodeToExclude = SycodeToExclude.replace(np.nan, '', regex=True)
      SycodeToExclude = SycodeToExclude.drop_duplicates()
    else:
      TraderMapping = file = pd.DataFrame(r.json()['values'][1:], columns=r.json()['values'][0])
      TraderMapping.columns = map(str, TraderMapping.columns.str.replace(' ', ''))
      TraderMapping = TraderMapping.replace({ "": np.nan})
      TraderMapping.dropna(axis=0, how='all', thresh=None, subset=None, inplace=True)
      TraderMapping = TraderMapping.replace(np.nan, '', regex=True)
      TraderMapping = TraderMapping.drop_duplicates()
  
  from pyspark.sql.types import *

  schema = StructType([StructField("MEDate", StringType(), True)\
                   ,StructField("Day1", StringType(), True)\
                   ,StructField("Day2", StringType(), True)])

  spark_df = spark.createDataFrame(PricingDays, schema=schema)

  spark_df.createOrReplaceTempView('lookup_PricingDays')
  
  tblList=spark.sql("""show tables in pricing""")
  table_name=tblList.filter(tblList.tableName=="riskpricinglookupfilespricingdays").collect()
  if len(table_name)>0:
    print("Table exists")
    q='''select count(*) from pricing.riskpricinglookupfilesPricingDays'''
    s=spark.sql(q).toPandas()
    present=s['count(1)'].iloc[0]
    if (not(present)):
      spark.sql('''insert into table pricing.riskpricinglookupfilespricingdays select MEDate, Day1, Day2 from lookup_PricingDays''')
    
    else:
      print("Table present")
      q='''delete from pricing.riskpricinglookupfilespricingdays'''
      spark.sql(q)
      spark.sql('''insert into table pricing.riskpricinglookupfilespricingdays select MEDate, Day1, Day2 from lookup_PricingDays ''')
    
  else:
    print("Table does not Exist")
    spark.sql('''create table if not exists pricing.riskpricinglookupfilespricingdays using delta select * from lookup_PricingDays''')
    
  
  
  schema = StructType([StructField("GenevaSycode", StringType(), True)\
                   ,StructField("Reason", StringType(), True)])

  spark_df = spark.createDataFrame(SycodeToExclude, schema=schema)

  spark_df.createOrReplaceTempView('lookup_SycodeToExclude')
  
  tblList=spark.sql("""show tables in pricing""")
  table_name=tblList.filter(tblList.tableName=="riskpricinglookupfilessycodetoexclude").collect()
  if len(table_name)>0:
    print("Table exists")
    q='''select count(*) from pricing.riskpricinglookupfilessycodetoexclude'''
    s=spark.sql(q).toPandas()
    present=s['count(1)'].iloc[0]
    if (not(present)):
      spark.sql('''insert into table pricing.riskpricinglookupfilessycodetoexclude select GenevaSycode, Reason from lookup_SycodeToExclude''')
    
    else:
      print("Table present")
      q1='''delete from pricing.riskpricinglookupfilessycodetoexclude'''
      spark.sql(q1)
#       q1='''delete from pricing.riskpricinglookupfilessycodetoexclude where GenevaSycode == "{}" and Reason == "{}"'''
#       spark.sql(q1.format(SycodeToExclude.GenevaSycode[0],SycodeToExclude.Reason[0]))
      spark.sql('''insert into table pricing.riskpricinglookupfilessycodetoexclude select GenevaSycode, Reason from lookup_SycodeToExclude''')
    
  else:
    print("Table does not Exist")
    spark.sql('''create table if not exists pricing.riskpricinglookupfilessycodetoexclude using delta select * from lookup_SycodeToExclude''')
  
  
  schema = StructType([StructField("Sycode", StringType(), True)\
                   ,StructField("Trader", StringType(), True)])

  spark_df = spark.createDataFrame(TraderMapping, schema=schema)

  spark_df.createOrReplaceTempView('lookup_TraderMapping')
  
  sycodeList = TraderMapping.Sycode.tolist()
  sycodeList = tuple(sycodeList)
  traderList = TraderMapping.Trader.tolist()
  traderList = tuple(traderList)
  
  tblList=spark.sql("""show tables in pricing""")
  table_name=tblList.filter(tblList.tableName=="riskpricinglookupfilestradermapping").collect()
  if len(table_name)>0:
    print("Table exists")
    q='''select count(*) from pricing.riskpricinglookupfilestradermapping'''
    s=spark.sql(q).toPandas()
    present=s['count(1)'].iloc[0]
    if (not(present)):
      spark.sql('''insert into table pricing.riskpricinglookupfilestradermapping select Sycode, Trader from lookup_TraderMapping''')
    
    else:
      print("Table present")
      q='''delete from pricing.riskpricinglookupfilestradermapping'''
      spark.sql(q)
#       q = '''delete from pricing.riskpricinglookupfilestradermapping where Sycode IN {} and Trader IN {}'''
#       spark.sql(q.format(sycodeList, traderList))
      spark.sql('''insert into table pricing.riskpricinglookupfilestradermapping select Sycode, Trader from lookup_TraderMapping ''')
    
  else:
    print("Table does not Exist")
    spark.sql('''create table if not exists pricing.riskpricinglookupfilestradermapping using delta select * from lookup_TraderMapping''')
  
  

else:
  pass

  

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Count(*) as Total from pricing.riskpricinglookupfilesPricingDays

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from pricing.riskpricinglookupfilesPricingDays

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Count(*) as Total from pricing.riskpricinglookupfilessycodetoexclude

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from pricing.riskpricinglookupfilessycodetoexclude

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Count(*) as Total from pricing.riskpricinglookupfilesTraderMapping

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from pricing.riskpricinglookupfilesTraderMapping

# COMMAND ----------

########################################################################## For RMBS types of Files ##############################################################################

# COMMAND ----------

if (len(otherFileDict) > 0):
  nameList = list()
  df = pd.DataFrame()
  for files in otherFileDict:
    url = "https://graph.microsoft.com/v1.0/sites/sharepointlocation/drive/items/{}/workbook/worksheets/sheetname/usedRange".format(otherFileDict[files])
    r = requests.get(url, headers=headers)
    df1 = pd.DataFrame(r.json()['values'][1:], columns=r.json()['values'][0])
    df1 = df1.replace({ "": np.nan})
    df1.dropna(axis=0, how='all', thresh=None, subset=None, inplace=True)
    df1 = df1.replace(np.nan, '', regex=True)
    df1.set_axis(["Geneva Sycode", "CUSIP", "ISIN", "IDC", "PD", "Avg Broker PX", "AGMark", "Action", "Reason"],axis=1,inplace=True)
    df1 = df1.assign(Type=files)
    df1 = df1.assign(DateAsOf=PricingDays.MEDate[0])
#     df1['IDC'] = df1['IDC'].astype(str)
#     df1['PD'] = df1['PD'].astype(str)
#     df1['AGMark'] = df1['AGMark'].astype(str)
    name = str.split(files, '- ')[1]
    name = str.split(name, '.xlsx')[0]
    nameList.append(name.capitalize())
    df1['Type'] = df1['Type'].str.split('.xlsx', expand=True)[0]
    df1['Type'] = df1['Type'].str.split('- ', expand=True)[1]
    df1['Type'] = df1['Type'].str.capitalize()
    df1.columns = map(str, df1.columns.str.replace(' ', ''))
    df1 = df1.drop_duplicates()
    df = pd.concat([df, df1])
   
  
  from pyspark.sql.types import *
    
  schema = StructType([StructField("GenevaSycode", StringType(), True)\
            ,StructField("CUSIP", StringType(), True)\
            ,StructField("ISIN", StringType(), True)\
            , StructField("IDC", StringType(), True)\
            ,StructField("PD", StringType(), True)\
            ,StructField("AvgBrokerPX", StringType(), True)\
             ,StructField("AGMark", StringType(), True)\
             ,StructField("Action", StringType(), True)\
             ,StructField("Reason", StringType(), True)\
             ,StructField("Type", StringType(), True)\
             ,StructField("DateAsOf", StringType(), True)])

  spark_df = spark.createDataFrame(df, schema=schema)

  spark_df.createOrReplaceTempView('SourcePricing')
  
  tblList=spark.sql("""show tables in pricing""")
  
  table_name=tblList.filter(tblList.tableName=="risksourcepricingfiles").collect()
  if len(table_name)>0:
    print("Table exists")
    q='''select count(*) from pricing.risksourcepricingfiles'''
    s=spark.sql(q).toPandas()
    present=s['count(1)'].iloc[0]
    if (not(present)):
      spark.sql('''insert into table pricing.risksourcepricingfiles select GenevaSycode, CUSIP, ISIN, IDC, PD, AvgBrokerPX, AGMark, Action, Reason, Type, DateAsOf from SourcePricing''')
    
    else:
      print("Table present")
      for i in range(len(nameList)):
        q='''delete from pricing.risksourcepricingfiles where Type == "{}"'''
        spark.sql(q.format(nameList[i]))
      spark.sql('''insert into table pricing.risksourcepricingfiles select GenevaSycode, CUSIP, ISIN, IDC, PD, AvgBrokerPX, AGMark, Action, Reason, Type, DateAsOf from SourcePricing ''')
    
  else:
    print("Table does not Exist")
    spark.sql('''create table if not exists pricing.risksourcepricingfiles using delta partitioned by (Type) select * from SourcePricing''')
else:
  pass
    

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Count(*) as Total from pricing.risksourcepricingfiles

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from pricing.risksourcepricingfiles

# COMMAND ----------

########################################################################### For Csv types of files ###################################################################################

# COMMAND ----------

if (len(csvFileDict) > 0):
  nameList = list()
  df = pd.DataFrame()
  for files in csvFileDict:
    url = "https://graph.microsoft.com/v1.0/sites/sharepointlocation/drive/items/{}".format(csvFileDict[files])
    r = requests.get(url, headers=headers)
    url1 = r.json()['@microsoft.graph.downloadUrl']
    r1 = requests.get(url1, headers=headers)
    decoded_content = r1.content.decode('utf-8')
    cr1 = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list1 = list(cr1)
    df2 = pd.DataFrame(my_list1)
    df2.columns = df2.iloc[3]
    df2 = df2.iloc[4:].reset_index(drop=True)
    df2 = df2.replace({ "": np.nan})
    df2.dropna(axis=0, how='all', thresh=None, subset=None, inplace=True)
    df2 = df2.replace(np.nan, '', regex=True)
    df2.set_axis(["Geneva Sycode", "CUSIP", "ISIN", "IDC", "PD", "Avg Broker PX", "AGMark", "Action", "Reason"],axis=1,inplace=True)
    df2 = df2.assign(Type=files)
    df2 = df2.assign(DateAsOf=PricingDays.MEDate[0])
#     df1['IDC'] = df1['IDC'].astype(str)
#     df1['PD'] = df1['PD'].astype(str)
#     df1['AGMark'] = df1['AGMark'].astype(str)
    name = str.split(files, 'Price')[1]
    name = str.split(name, '.csv')[0]
    nameList.append(name.capitalize())
    df2['Type'] = df2['Type'].str.split('.csv', expand=True)[0]
    df2['Type'] = df2['Type'].str.split('Price', expand=True)[1]
    df2['Type'] = df2['Type'].str.capitalize()
    df2.columns = map(str, df2.columns.str.replace(' ', ''))
    df2 = df2.drop_duplicates()
    df = pd.concat([df, df2])
   
  
  from pyspark.sql.types import *
    
  schema = StructType([StructField("GenevaSycode", StringType(), True)\
            ,StructField("CUSIP", StringType(), True)\
            ,StructField("ISIN", StringType(), True)\
            , StructField("IDC", StringType(), True)\
            ,StructField("PD", StringType(), True)\
            ,StructField("AvgBrokerPX", StringType(), True)\
             ,StructField("AGMark", StringType(), True)\
             ,StructField("Action", StringType(), True)\
             ,StructField("Reason", StringType(), True)\
             ,StructField("Type", StringType(), True)\
             ,StructField("DateAsOf", StringType(), True)])

  spark_df = spark.createDataFrame(df, schema=schema)

  spark_df.createOrReplaceTempView('SourcePricing')
  
  tblList=spark.sql("""show tables in pricing""")
  
  table_name=tblList.filter(tblList.tableName=="risksourcepricingfiles").collect()
  if len(table_name)>0:
    print("Table exists")
    q='''select count(*) from pricing.risksourcepricingfiles'''
    s=spark.sql(q).toPandas()
    present=s['count(1)'].iloc[0]
    if (not(present)):
      spark.sql('''insert into table pricing.risksourcepricingfiles select GenevaSycode, CUSIP, ISIN, IDC, PD, AvgBrokerPX, AGMark, Action, Reason, Type, DateAsOf from SourcePricing''')
    
    else:
      print("Table present")
      for i in range(len(nameList)):
        q='''delete from pricing.risksourcepricingfiles where Type == "{}"'''
        spark.sql(q.format(nameList[i]))
      spark.sql('''insert into table pricing.risksourcepricingfiles select GenevaSycode, CUSIP, ISIN, IDC, PD, AvgBrokerPX, AGMark, Action, Reason, Type, DateAsOf from SourcePricing ''')
    
  else:
    print("Table does not Exist")
    spark.sql('''create table if not exists pricing.risksourcepricingfiles using delta partitioned by (Type) select * from SourcePricing''')
else:
  pass

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Count(*) as Total from pricing.risksourcepricingfiles

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from pricing.risksourcepricingfiles

# COMMAND ----------


