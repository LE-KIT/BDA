#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os
import glob


# In[27]:


# PART: DENMARK

dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') # Change dateformat

# Data Import - DENMARK
#path = r'C:\\Data\\Uni\\KIT\\Master\\BWL\\BWL3_BDA\\Datenquellen\\DENMARK'
path = 'data/DENMARK'
all_Sonnenstunden = glob.glob(path + "\Sonnenstunden*.csv")
all_Wind = glob.glob(path + "\Wind*.csv")


# In[28]:


# Data Import - DENMARK: sun
list_sun = []
for filename in all_Sonnenstunden:
    df = pd.read_csv(filename, 
                     sep=';', 
                     decimal=",", 
                     thousands=".", 
                     parse_dates=['DateTime'], 
                     date_parser=dateparse,
                     index_col=None, 
                     header=0)
    df.columns = df.columns.str.strip().str.lower()
    df.rename(columns={'datetime':'date', 'sol':'DK_sun_hrs'}, inplace=True)
    list_sun.append(df)

sun_DK = pd.concat(list_sun, axis=0, ignore_index=True) # append dataframes

print(sun_DK.head())
print(sun_DK.describe())
print(sun_DK.info())


# In[29]:


# Data Import - DENMARK: wind
list_wind = []
for filename in all_Wind:
    df = pd.read_csv(filename, 
                     sep=';', 
                     decimal=",", 
                     thousands=".", 
                     parse_dates=['DateTime'], 
                     date_parser=dateparse,
                     index_col=None, 
                     header=0)
    df.columns = df.columns.str.strip().str.lower()
    df.rename(columns={'datetime':'date', 
                       'middel vindhastighed':'DK_av_windSpeed_ms', 
                       'højeste 10 min. middel':'DK_maxAvg_windSpeed_ms', 
                       'højeste vindstød':'DK_max_windSpeed_ms'}, 
              inplace=True)
    list_wind.append(df)

wind_DK = pd.concat(list_wind, axis=0, ignore_index=True)

print(wind_DK.head())
print(wind_DK.describe())
print(wind_DK.info())


# In[34]:


# dataframe DENMARK with mean values per day

denmark = sun_DK.merge(wind_DK, how='outer', on='date').drop(columns=['DK_maxAvg_windSpeed_ms', 'DK_max_windSpeed_ms'])
print(denmark.head())


# In[26]:


# PART: GERMANY

# Data Import - GERMANY
#path = r'C:\\Data\\Uni\\KIT\\Master\\BWL\\BWL3_BDA\\Datenquellen\\GERMANY'
path = 'data/GERMANY'

data_climate = glob.glob(path + "\produkt_klima_tag_*.txt")

ls = []
for filename in data_climate:
    df = pd.read_csv(filename,
                     sep=';',
                     na_values='-999',
                     header=0,
                     usecols=[0,1,4,6,8],
                     dtype={'STATIONS_ID':str},
                     parse_dates=['MESS_DATUM'])
    df.columns = df.columns.str.strip().str.lower()
    df.rename(columns={'stations_id': 'id', 
             'mess_datum': 'date', 
             'fm': 'DE_av_windspeed_ms', 
             'rsk': 'DE_percip_mm',
             'sdk': 'DE_sun_hrs'},
            inplace=True)
    df['id'] = df['id'].str.strip()
    ls.append(df)

climate_DE = pd.concat(ls, axis=0, ignore_index=True) # append dataframes

print(climate_DE.head())
print(climate_DE.describe())
print(climate_DE.info())


# In[25]:


# dataframe GERMANY with mean values per day

germany = climate_DE.groupby('date').apply(lambda group: group.mean(skipna = True)).drop(columns=['id'])
print(germany.head())

