#!/usr/bin/env python
# coding: utf-8

from scripts.datetimeManipulation import make_hourly

import pandas as pd
import glob


def import_weatherData_DE(path = 'data/GERMANY/', regionsFile = 'KL_Wetterstationen_DE.txt'):

    # Data Sources
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

    climate_DE = climate_DE.rename(columns={'STATIONS_ID': 'id',
                               'MESS_DATUM': 'date',
                               '  FM': 'DE_av_windspeed_ms',
                               ' RSK': 'DE_percip_mm',
                               ' SDK': 'DE_sun_hrs'})

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

    sun_DK.rename(columns={'DateTime': 'date', 'Sol': 'DK_sun_hrs'}, inplace=True)

    # Make hourly
    sun_DK = make_hourly(sun_DK)

    # [Hours] --> divide sunny hours by 24
    sun_DK['DK_sun_hrs'] = sun_DK['DK_sun_hrs'] / 24

    # Time format : 24-01-2018 12:34
    sun_DK['date'] = sun_DK['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

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

    wind_DK.rename(columns={'DateTime': 'date',
                            'Middel vindhastighed': 'DK_av_windSpeed_ms',
                            'Højeste 10 min. middel': 'DK_maxAvg_windSpeed_ms',
                            'Højeste vindstød': 'DK_max_windSpeed_ms'},
                   inplace=True)

    # Make hourly
    wind_DK = make_hourly(wind_DK)

    # [AVG Values] --> no further manipulation necessary

    # Time format : 24-01-2018 12:34
    wind_DK['date'] = wind_DK['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

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

    # Average values per hour (already AVG values per 30 min)--> MEAN
    weather_FR = weather_FR.set_index('valid').resample('H').mean().drop(columns=['peak_wind_time']).reset_index()
    weather_FR.rename(columns={'valid': 'date',
                               'tmpc': 'FR_av_temperatureChange',
                               'sped': 'FR_av_windSpeed'},
                      inplace=True)

    # Time format : 24-01-2018 12:34
    weather_FR['date'] = weather_FR['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

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

    # Average values per hour (already AVG values per 30 min)--> MEAN
    weather_CZ = weather_CZ.set_index('valid').resample('H').mean().drop(columns=['peak_wind_time']).reset_index()
    weather_CZ.rename(columns={'valid': 'date',
                               'tmpc': 'CZ_av_temperatureChange',
                               'sped': 'CZ_av_windSpeed'},
                      inplace=True)

    # Time format : 24-01-2018 12:34
    weather_CZ['date'] = weather_CZ['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    return weather_CZ

def import_weatherData():

    weatherData = pd.merge(left=import_weatherData_FR(),
                            right=pd.merge(
                                        left=import_weatherData_DK(),
                                        right=import_weatherData_CZ(),
                                        on='date',
                                        how='outer'),
                            on='date',
                            how='outer')#.merge(right=import_weatherData_DE(),
                                         #      on='date',
                                          #     how='outer')


    return weatherData

df = import_weatherData()

