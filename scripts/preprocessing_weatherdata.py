#!/usr/bin/env python
# coding: utf-8

import glob

import pandas as pd

from scripts.datetimeManipulation import make_hourly


def import_weatherData_DE(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/', path='data/GERMANY/',
                          regionsFile='KL_Wetterstationen_DE.txt'):
    print('Starting import_weatherData_DE')

    dateparse = lambda x: pd.datetime.strptime(x, '%Y%m%d%H')  # Change dateformat

    # Data Sources
    all_Sonne = glob.glob(projectPath + path + "produkt_sd_stunde_*.txt")
    all_Wind = glob.glob(projectPath + path + "produkt_ff_stunde_*.txt")
    all_Temperatur = glob.glob(projectPath + path + "produkt_tu_stunde_*.txt")

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
        print('Finished importing {}. rawdata for GERMANY: SUN of {} files'.format(i + 1, len(all_Sonne)))

    sun_DE.columns = sun_DE.columns.str.strip().str.lower()
    sun_DE.rename(columns={'stations_id': 'id',
                           'mess_datum': 'date',
                           'sd_so': 'DE_sun_min'},
                  inplace=True)
    sun_DE['id'] = sun_DE['id'].str.strip()

    # Time format : 24-01-2018 12:34
    # sun_DE['date'] = sun_DE['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    sun_DE = sun_DE[sun_DE['date'] >= pd.to_datetime('01-06-2015 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    sun_DE['Dummy'] = 1

    if len(sun_DE.groupby(['date', 'id']).count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        sun_DE = sun_DE.groupby(['date', 'id']).mean().reset_index()

    sun_DE.drop('Dummy', axis=1, inplace=True)

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
        print('Finished importing {}. rawdata for GERMANY: WIND of {} files'.format(i + 1, len(all_Wind)))

    wind_DE.columns = wind_DE.columns.str.strip().str.lower()
    wind_DE.rename(columns={'stations_id': 'id',
                            'mess_datum': 'date',
                            'f': 'DE_wind_ms'},
                   inplace=True)
    wind_DE['id'] = wind_DE['id'].str.strip()

    # Time format : 24-01-2018 12:34
    # wind_DE['date'] = wind_DE['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    wind_DE = wind_DE[wind_DE['date'] >= pd.to_datetime('01-06-2015 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    wind_DE['Dummy'] = 1

    if len(wind_DE.groupby(['date', 'id']).count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        wind_DE = wind_DE.groupby(['date', 'id']).mean().reset_index()

    wind_DE.drop('Dummy', axis=1, inplace=True)

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
        print('Finished importing {}. rawdata for GERMANY: TEMPERATURE of {} files'.format(i + 1, len(all_Temperatur)))

    temp_DE.columns = temp_DE.columns.str.strip().str.lower()
    temp_DE.rename(columns={'stations_id': 'id',
                            'mess_datum': 'date',
                            'tt_tu': 'DE_temp_C'},
                   inplace=True)
    temp_DE['id'] = temp_DE['id'].str.strip()

    # Time format : 24-01-2018 12:34
    # temp_DE['date'] = temp_DE['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    temp_DE = temp_DE[temp_DE['date'] >= pd.to_datetime('01-06-2015 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    temp_DE['Dummy'] = 1

    if len(temp_DE.groupby(['date', 'id']).count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        temp_DE = temp_DE.groupby(['date', 'id']).mean().reset_index()

    temp_DE.drop('Dummy', axis=1, inplace=True)

    # Merge data - GERMANY
    climate_DE = wind_DE.merge(right=sun_DE,
                               on=['id', 'date'],
                               how='outer').merge(right=temp_DE,
                                                  on=['id', 'date'],
                                                  how='outer')  # ,indicator=True)

    # Weather stations and north-south distinction
    stations = pd.read_csv(projectPath + path + regionsFile,
                           sep=';',
                           encoding='latin-1',
                           dtype={'Station': str},
                           usecols=[0, 1])
    stations.rename(columns={'Station': 'id'}, inplace=True)
    stations['id'] = stations['id'].str.lstrip('0')

    df_DE = pd.merge(left=climate_DE, right=stations, on='id', how='left')
    df_tmp = df_DE.groupby(['date', 'Region']).mean().reset_index()

    germany = pd.merge(left=df_tmp[df_tmp['Region'] == 'n'].drop(columns=['Region']),
                       right=df_tmp[df_tmp['Region'] == 's'].drop(columns=['Region']),
                       on='date',
                       how='outer',
                       suffixes=['_north', '_south'])

    print('Finished import_weatherData_DE')

    return germany


def import_weatherData_DK(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/', path='data/DENMARK/'):
    print('Starting import_weatherData_DK')

    # Data Sources
    all_Sonnenstunden = glob.glob(projectPath + path + "Sonnenstunden*.csv")
    all_Wind = glob.glob(projectPath + path + "Wind*.csv")

    # Data Import - DENMARK: sun

    for i in range(len(all_Sonnenstunden)):
        if i == 0:
            sun_DK = pd.read_csv(all_Sonnenstunden[i],
                                 sep=';',
                                 decimal=",",
                                 thousands=".",
                                 parse_dates=['DateTime'],
                                 index_col=None,
                                 header=0)
        else:
            sun_DK = sun_DK.append(pd.read_csv(all_Sonnenstunden[i],
                                               sep=';',
                                               decimal=",",
                                               thousands=".",
                                               parse_dates=['DateTime'],
                                               index_col=None,
                                               header=0),
                                   ignore_index=True)

    sun_DK.rename(columns={'DateTime': 'date', 'Sol': 'DK_sun_hrs'}, inplace=True)

    # Make hourly
    sun_DK = make_hourly(sun_DK)

    # [Hours] --> divide sunny hours by 24
    sun_DK['DK_sun_hrs'] = sun_DK['DK_sun_hrs'] / 24

    # Time format : 24-01-2018 12:34
    # sun_DK['date'] = sun_DK['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    sun_DK = sun_DK[sun_DK['date'] >= pd.to_datetime('01-06-2015 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    sun_DK['Dummy'] = 1

    if len(sun_DK.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        sun_DK = sun_DK.groupby('date').mean().reset_index()

    sun_DK.drop('Dummy', axis=1, inplace=True)

    # Data Import - DENMARK: wind
    for i in range(len(all_Wind)):
        if i == 0:
            wind_DK = pd.read_csv(all_Wind[i],
                                  sep=';',
                                  decimal=",",
                                  thousands=".",
                                  parse_dates=['DateTime'],
                                  index_col=None,
                                  header=0)
        else:
            wind_DK = wind_DK.append(pd.read_csv(all_Wind[i],
                                                 sep=';',
                                                 decimal=",",
                                                 thousands=".",
                                                 parse_dates=['DateTime'],
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
    # wind_DK['date'] = wind_DK['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    wind_DK = wind_DK[wind_DK['date'] >= pd.to_datetime('01-06-2015 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    wind_DK['Dummy'] = 1

    if len(wind_DK.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        wind_DK = wind_DK.groupby('date').mean().reset_index()

    wind_DK.drop('Dummy', axis=1, inplace=True)

    denmark = sun_DK.merge(wind_DK, how='outer', on='date').drop(
        columns=['DK_maxAvg_windSpeed_ms', 'DK_max_windSpeed_ms'])

    print('Finished import_weatherData_DK')

    return denmark


def import_weatherData_FR(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/',
                          file='data/FRANCE/weather_fr.txt'):
    print('Starting import_weatherData_FR')

    # Data Import - FRANCE
    weather_FR = pd.read_csv(projectPath + file,
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
    # weather_FR['date'] = weather_FR['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    weather_FR = weather_FR[weather_FR['date'] >= pd.to_datetime('01-06-2015 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    weather_FR['Dummy'] = 1

    if len(weather_FR.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        weather_FR = weather_FR.groupby('date').mean().reset_index()

    weather_FR.drop('Dummy', axis=1, inplace=True)

    print('Finished import_weatherData_FR')

    return weather_FR


def import_weatherData_CZ(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/',
                          file='data/CZECH REPUBLIC/weather_cz.txt'):
    print('Starting import_weatherData_CZ')

    # Data Import - CZECH REPUBLIC
    weather_CZ = pd.read_csv(projectPath + file,
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
    # weather_CZ['date'] = weather_CZ['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    weather_CZ = weather_CZ[weather_CZ['date'] >= pd.to_datetime('01-06-2015 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    weather_CZ['Dummy'] = 1

    if len(weather_CZ.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        weather_CZ = weather_CZ.groupby('date').mean().reset_index()

    weather_CZ.drop('Dummy', axis=1, inplace=True)

    print('Finished import_weatherData_CZ')

    return weather_CZ


def import_weatherData(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/'):
    print('Starting import_weatherData')

    weatherData = pd.merge(left=import_weatherData_FR(projectPath),
                           right=pd.merge(
                               left=import_weatherData_DK(projectPath),
                               right=import_weatherData_CZ(projectPath),
                               on='date',
                               how='outer'),
                           on='date',
                           how='outer').merge(right=import_weatherData_DE(projectPath),
                                              on='date',
                                              how='outer')

    print('Finished import_weatherData')

    return weatherData
