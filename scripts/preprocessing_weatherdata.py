#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import glob


def import_weatherData_DE(path = 'data/GERMANY/', regionsFile = 'KL_Wetterstationen_DE.txt'):

    # Data Sources
    #path = 'data/GERMANY/'
    all_Wetter = glob.glob(path + "produkt_klima_tag_*.txt")

    # Data Import - GERMANY
    for i in range(len(all_Wetter)):
        if i == 0:
            climate_DE = pd.read_csv(all_Wetter[i],
                                     sep=';',
                                     na_values='-999',
                                     header=0,
                                     usecols=[0, 1, 4, 6, 8],
                                     dtype={'STATIONS_ID': str},
                                     parse_dates=['MESS_DATUM'])
        else:
            climate_DE = climate_DE.append(pd.read_csv(all_Wetter[i],
                                                       sep=';',
                                                       na_values='-999',
                                                       header=0,
                                                       usecols=[0, 1, 4, 6, 8],
                                                       dtype={'STATIONS_ID': str},
                                                       parse_dates=['MESS_DATUM']),
                                           ignore_index=True)
    climate_DE.columns = climate_DE.columns.str.strip().str.lower()
    climate_DE.rename(columns={'stations_id': 'id',
                               'mess_datum': 'date',
                               'fm': 'DE_av_windspeed_ms',
                               'rsk': 'DE_percip_mm',
                               'sdk': 'DE_sun_hrs'},
                      inplace=True)
    climate_DE['id'] = climate_DE['id'].str.strip()

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

def import_weatherData_DK(path = 'data/DENMARK/'):

    # Data Sources
    #path = 'data/DENMARK/'
    all_Sonnenstunden = glob.glob(path + "Sonnenstunden*.csv")
    all_Wind = glob.glob(path + "Wind*.csv")

    # Data Import - DENMARK: sun
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')  # Change dateformat

    for i in range(len(all_Sonnenstunden)):
        if i == 0:
            sun_DK = pd.read_csv(all_Sonnenstunden[i],
                                 sep=';',
                                 decimal=",",
                                 thousands=".",
                                 parse_dates=['DateTime'],
                                 date_parser=dateparse,
                                 index_col=None,
                                 header=0)
        else:
            sun_DK = sun_DK.append(pd.read_csv(all_Sonnenstunden[i],
                                               sep=';',
                                               decimal=",",
                                               thousands=".",
                                               parse_dates=['DateTime'],
                                               date_parser=dateparse,
                                               index_col=None,
                                               header=0),
                                   ignore_index=True)

    sun_DK.columns = sun_DK.columns.str.strip().str.lower()
    sun_DK.rename(columns={'datetime': 'date', 'sol': 'DK_sun_hrs'}, inplace=True)

    # Data Import - DENMARK: wind
    for i in range(len(all_Wind)):
        if i == 0:
            wind_DK = pd.read_csv(all_Wind[i],
                                  sep=';',
                                  decimal=",",
                                  thousands=".",
                                  parse_dates=['DateTime'],
                                  date_parser=dateparse,
                                  index_col=None,
                                  header=0)
        else:
            wind_DK = wind_DK.append(pd.read_csv(all_Wind[i],
                                                 sep=';',
                                                 decimal=",",
                                                 thousands=".",
                                                 parse_dates=['DateTime'],
                                                 date_parser=dateparse,
                                                 index_col=None,
                                                 header=0),
                                     ignore_index=True)

    wind_DK.columns = wind_DK.columns.str.strip().str.lower()
    wind_DK.rename(columns={'datetime': 'date',
                            'middel vindhastighed': 'DK_av_windSpeed_ms',
                            'højeste 10 min. middel': 'DK_maxAvg_windSpeed_ms',
                            'højeste vindstød': 'DK_max_windSpeed_ms'},
                   inplace=True)

    # dataframe DENMARK with mean values per day

    denmark = sun_DK.merge(wind_DK, how='outer', on='date').drop(columns=['DK_maxAvg_windSpeed_ms', 'DK_max_windSpeed_ms'])

    return denmark

def import_weatherData_FR(file = 'data/FRANCE/weather_fr.txt'):

    # Data Import - FRANCE
    weather_FR = pd.read_csv(file,
                             sep=",",
                             decimal=".",
                             parse_dates=['valid'],
                             date_parser=lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M'),
                             index_col=None,
                             header=0)

    # Average valuesper day
    weather_FR = weather_FR.set_index('valid').resample('D').mean().drop(columns=['peak_wind_time']).reset_index()
    weather_FR.rename(columns={'valid': 'date',
                               'tmpc': 'FR_av_temperatureChange',
                               'sped': 'FR_av_windSpeed'},
                      inplace=True)


    return weather_FR

def import_weatherData_CZ(file = 'data/CZECH REPUBLIC/weather_cz.txt'):

    # Data Import - CZECH REPUBLIC
    weather_CZ = pd.read_csv(file,
                             sep=",",
                             decimal=".",
                             parse_dates=['valid'],
                             date_parser=lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M'),
                             index_col=None,
                             header=0)

    # Average valuesper day
    weather_CZ = weather_CZ.set_index('valid').resample('D').mean().drop(columns=['peak_wind_time']).reset_index()
    weather_CZ.rename(columns={'valid': 'date',
                               'tmpc': 'CZ_av_temperatureChange',
                               'sped': 'CZ_av_windSpeed'},
                      inplace=True)


    return weather_CZ

def import_weatherData():

    weatherData = pd.merge(left=import_weatherData_DE(),
                            right=pd.merge(
                                        left=import_weatherData_DK(),
                                        right=import_weatherData_CZ(),
                                        on='date',
                                        how='outer'),
                            on='date',
                            how='outer'
                        ).merge(right=import_weatherData_FR(),
                                on='date',
                                how='outer')
    return weatherData


