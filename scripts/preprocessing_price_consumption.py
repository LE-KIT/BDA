#!/usr/bin/env python
# coding: utf-8

import glob

import numpy as np
import pandas as pd


def import_priceData(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/', path='data/strompreise/'):
    print('Starting import_priceData')

    files = glob.glob(projectPath + path + "DE_Großhandelspreise*.csv")

    numberparse = lambda x: pd.np.float(x.replace(".", "").replace(",", ".")) if x != "-" else np.nan
    dateparse = lambda x: pd.datetime.strptime(x, '%d.%m.%Y %H:%M')

    for i in range(len(files)):
        if i == 0:
            df_price = pd.read_csv(files[i],
                                   sep=";",
                                   decimal=r",",
                                   thousands=r".",
                                   converters={num: numberparse for num in np.arange(2, 22)},
                                   parse_dates=[['Datum', 'Uhrzeit']],
                                   date_parser=dateparse)
        else:
            df_price = df_price.append(pd.read_csv(files[i],
                                                   sep=";",
                                                   decimal=r",",
                                                   thousands=r".",
                                                   converters={num: numberparse for num in np.arange(2, 22)},
                                                   parse_dates=[['Datum', 'Uhrzeit']],
                                                   date_parser=dateparse))

    cols = ['date']
    for i in df_price.columns[1:]:
        cols.append('price_' + i.replace('[Euro/MWh]', '_Euro_MWh'))
    df_price.columns = cols

    df_price.drop_duplicates(inplace=True)

    # Fill NaN with mean --> ???
    df_price.fillna(df_price.mean(), inplace=True)

    # Time format : 24-01-2018 12:34
    # df_price['date'] = df_price['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # No shift necessary
    # ---

    # Delete unnecessary rows
    df_price = df_price[df_price['date'] >= pd.to_datetime('01-06-2015 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    df_price['Dummy'] = 1

    if len(df_price.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        df_price = df_price.groupby('date').mean().reset_index()

    df_price.drop('Dummy', axis=1, inplace=True)

    print('Finished import_priceData')

    return df_price


def import_consumptionData(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/',
                           path='data/Stromverbrauch_real/'):
    print('Starting import_consumptionData')

    files = glob.glob(projectPath + path + "DE_Realisierter Stromverbrauch*.csv")

    numberparse = lambda x: pd.np.float(x.replace(".", "").replace(",", ".")) if x != "-" else np.nan
    dateparse = lambda x: pd.datetime.strptime(x, '%d.%m.%Y %H:%M')

    for i in range(len(files)):
        if i == 0:
            df_consumption = pd.read_csv(files[i],
                                         sep=";",
                                         decimal=r",",
                                         thousands=r".",
                                         converters={num: numberparse for num in np.arange(2, 22)},
                                         parse_dates=[['Datum', 'Uhrzeit']],
                                         date_parser=dateparse)
        else:
            df_consumption = df_consumption.append(pd.read_csv(files[i],
                                                               sep=";",
                                                               decimal=r",",
                                                               thousands=r".",
                                                               converters={num: numberparse for num in
                                                                           np.arange(2, 22)},
                                                               parse_dates=[['Datum', 'Uhrzeit']],
                                                               date_parser=dateparse))

    df_consumption.rename(columns={'Datum_Uhrzeit': 'date',
                                   'Gesamt[MWh]': 'DE_consumption_MW'},
                          inplace=True)

    # Sum of the values per hour # [MWh] --> MEAN VS SUM --> SUM, due to weird calculation in rawdata
    df_consumption = df_consumption.set_index('date').resample('H').sum().reset_index()

    # Time format : 24-01-2018 12:34
    # df_consumption['date'] = df_consumption['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    df_consumption = df_consumption[
        df_consumption['date'] >= pd.to_datetime('01-06-2015 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    df_consumption['Dummy'] = 1

    if len(df_consumption.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        df_consumption = df_consumption.groupby('date').mean().reset_index()

    df_consumption.drop('Dummy', axis=1, inplace=True)

    print('Finished import_consumptionData')

    return df_consumption


def import_price_consumption_Data(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/'):
    print('Starting import_price_consumption_Data')

    df_price_consumption = pd.merge(left=import_priceData(projectPath),
                                    right=import_consumptionData(projectPath),
                                    on='date',
                                    how='outer')

    print('Finished import_price_consumption_Data')

    return df_price_consumption
