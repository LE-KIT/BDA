#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import glob


def import_priceData(path = 'data/strompreise/'):

    files = glob.glob(path + "DE_Großhandelspreise*.csv")

    numberparse = lambda x: pd.np.float(x.replace(".", "").replace(",", ".")) if x != "-" else np.nan

    for i in range(len(files)):
        if i == 0:
            df_price = pd.read_csv(files[i],
                        sep=";",
                        decimal=r",",
                        thousands=r".",
                        converters = {num: numberparse for num in np.arange(2, 22)},
                        parse_dates=[['Datum', 'Uhrzeit']])
        else:
            df_price = df_price.append(pd.read_csv(files[i],
                                sep=";",
                                decimal=r",",
                                thousands=r".",
                                converters = {num: numberparse for num in np.arange(2, 22)},
                                parse_dates=[['Datum', 'Uhrzeit']]))


    df_price.rename(columns={'Datum_Uhrzeit':'date'}, inplace=True)

    # Fill NaN with mean --> ???
    df_price.fillna(df_price.mean(), inplace=True)

    # Time format : 24-01-2018 12:34
    df_price['date'] = df_price['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # No shift necessary
    # ---

    return df_price


def import_consumptionData(path = 'data/Stromverbrauch_real/'):

    files = glob.glob(path + "DE_Realisierter Stromverbrauch*.csv")

    numberparse = lambda x: pd.np.float(x.replace(".", "").replace(",", ".")) if x != "-" else np.nan

    for i in range(len(files)):
        if i == 0:
            df_consumption = pd.read_csv(files[i],
                        sep=";",
                        decimal=r",",
                        thousands=r".",
                        converters = {num: numberparse for num in np.arange(2, 22)},
                        parse_dates=[['Datum', 'Uhrzeit']])
        else:
            df_consumption = df_consumption.append(pd.read_csv(files[i],
                                sep=";",
                                decimal=r",",
                                thousands=r".",
                                converters = {num: numberparse for num in np.arange(2, 22)},
                                parse_dates=[['Datum', 'Uhrzeit']]))

    df_consumption.rename(columns={'Datum_Uhrzeit':'date'}, inplace=True)

    # Sum of the values per hour # [MWh] --> MEAN VS SUM --> SUM, due to weird calculation in rawdata
    df_consumption = df_consumption.set_index('date').resample('H').sum().reset_index()

    # Time format : 24-01-2018 12:34
    df_consumption['date'] = df_consumption['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))


    return df_consumption


def import_price_consumption_Data():

    df_price_consumption = pd.merge(left=import_priceData(),
                                    right=import_consumptionData(),
                                      on='date',
                                      how='outer')

    return df_price_consumption










