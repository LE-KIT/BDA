#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import glob



def import_weatherData_DE(path = 'data/GERMANY/', regionsFile = 'KL_Wetterstationen_DE.txt'):

    dateparse = lambda x: pd.datetime.strptime(x, '%Y%m%d%H') # Change dateformat
    
    
    # Data Sources
    all_Sonne = glob.glob(path + "\produkt_sd_stunde_*.txt")
    all_Wind = glob.glob(path + "\produkt_ff_stunde_*.txt")
    all_Temperatur = glob.glob(path + "\produkt_tu_stunde_*.txt")

    
    # Data Import - GERMANY: SUN
    for i in range(len(all_Sonne)):
        if i == 0:
            sun_DE = pd.read_csv(all_Sonne[i],
                                 sep=';',
                                 na_values='-999',
                                 header=0,
                                 usecols=[0, 1, 3],
                                 dtype={'STATIONS_ID': str},
                                 parse_dates=['MESS_DATUM'],
                                 date_parser=dateparse)
        else:
            sun_DE = sun_DE.append(pd.read_csv(all_Sonne[i],
                                               sep=';',
                                               na_values='-999',
                                               header=0,
                                               usecols=[0, 1, 3],
                                               dtype={'STATIONS_ID': str},
                                               parse_dates=['MESS_DATUM'],
                                               date_parser=dateparse),
                                    ignore_index=True)
            
    sun_DE.columns = sun_DE.columns.str.strip().str.lower()
    sun_DE.rename(columns={'stations_id': 'id',
                           'mess_datum': 'date',
                           'sd_so': 'DE_sun_min'},
                  inplace=True)
    sun_DE['id'] = sun_DE['id'].str.strip()

    
    # Data Import - GERMANY: WIND
    for i in range(len(all_Wind)):
        if i == 0:
            wind_DE = pd.read_csv(all_Wind[i],
                                  sep=';',
                                  na_values='-999',
                                  header=0,
                                  usecols=[0, 1, 3],
                                  dtype={'STATIONS_ID': str},
                                  parse_dates=['MESS_DATUM'],
                                  date_parser=dateparse)
        else:
            wind_DE = wind_DE.append(pd.read_csv(all_Wind[i],
                                                 sep=';',
                                                 na_values='-999',
                                                 header=0,
                                                 usecols=[0, 1, 3],
                                                 dtype={'STATIONS_ID': str},
                                                 parse_dates=['MESS_DATUM'],
                                                 date_parser=dateparse),
                                     ignore_index=True)
            
    wind_DE.columns = wind_DE.columns.str.strip().str.lower()
    wind_DE.rename(columns={'stations_id': 'id',
                            'mess_datum': 'date',
                            'f': 'DE_wind_ms'},
                   inplace=True)
    wind_DE['id'] = wind_DE['id'].str.strip()
    
    
    # Data Import - GERMANY: TEMPERATURE
    for i in range(len(all_Temperatur)):
        if i == 0:
            temp_DE = pd.read_csv(all_Temperatur[i],
                                  sep=';',
                                  na_values='-999',
                                  header=0,
                                  usecols=[0, 1, 3],
                                  dtype={'STATIONS_ID': str},
                                  parse_dates=['MESS_DATUM'],
                                  date_parser=dateparse)
        else:
            temp_DE = temp_DE.append(pd.read_csv(all_Temperatur[i],
                                                 sep=';',
                                                 na_values='-999',
                                                 header=0,
                                                 usecols=[0, 1, 3],
                                                 dtype={'STATIONS_ID': str},
                                                 parse_dates=['MESS_DATUM'],
                                                 date_parser=dateparse),
                                     ignore_index=True)
            
    temp_DE.columns = temp_DE.columns.str.strip().str.lower()
    temp_DE.rename(columns={'stations_id': 'id',
                            'mess_datum': 'date',
                            'tt_tu': 'DE_temp_C'},
                   inplace=True)
    temp_DE['id'] = temp_DE['id'].str.strip()

    
    # Merge data - GERMANY
    climate_DE = pd.merge(wind_DE, sun_DE, on=['id', 'date'], how='outer')
    climate_DE = pd.merge(climate_DE, temp_DE, on=['id', 'date'], how='outer', indicator=True)
    climate_DE = climate_DE.sort_values(['id', 'date'], ascending=True)
    
  
    # Weather stations and north-south distinction
    stations = pd.read_csv(path+regionsFile,
                           sep=';',
                           encoding='latin-1',
                           dtype={'Station': str},
                           usecols=[0, 1])
    stations.rename(columns={'Station': 'id'}, inplace=True)
    stations['id'] = stations['id'].str.lstrip('0')

    df_DE = pd.merge(left=climate_DE, right=stations, how='left')
    df_tmp = df_DE.groupby(['date', 'Region']).mean().reset_index()


    germany = pd.merge(left=df_tmp[df_tmp['Region'] == 'n'].drop(columns=['Region']),
                       right=df_tmp[df_tmp['Region'] == 's'].drop(columns=['Region']),
                       on='date',
                       how='outer',
                       suffixes=['_north', '_south'])

    return germany

