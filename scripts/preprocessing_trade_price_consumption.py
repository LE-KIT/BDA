#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import glob
import re
import numpy as np

from scripts.preprocessing_old import preprocessing_stromverbrauch

numberparse = lambda x: pd.np.float(x.replace(".", "").replace(",",".")) if x!="-" else np.nan


def import_foreignTradeData(path = 'data/Produktion und Infrastruktur/'):

    files = glob.glob(path + "Kommerzieller_Au_enhandel*.csv")

    for i in range(len(files)):
        if i == 0:
            df_foreignTrade = pd.read_csv(files[i],
                                    sep=';',
                                    decimal=',',
                                    thousands='.',
                                    parse_dates=['Datum'],
                                    na_values='-')
        else:
            df_foreignTrade.append(pd.read_csv(files[i],
                                        sep=';',
                                        decimal=',',
                                        thousands='.',
                                        parse_dates=['Datum'],
                                        na_values='-'))


    df_foreignTrade.drop('Uhrzeit', axis=1, inplace=True)

    # Rename columns
    countries = {
        "Niederlande": "NL",
        "Schweiz": "CHE",
        "Dänemark": "DNK",
        "Tschechien": "CZE",
        "Luxemburg": "LUX",
        "Schweden": "SWE",
        "Österreich": "AUT",
        "Frankreich": "FRA",
        "Polen": "PL",
        }

    types = {"Import": "IM", "Export": "EX"}
    type_pattern = r"\((.*?)\)"
    country_pattern = r"(.*?) "
    cols = [countries.get(re.search(country_pattern, col).group(1))
            + "_"
            + types.get(re.search(type_pattern, col).group(1))
            for col in df_foreignTrade.columns[2::]]
    cols.insert(0, "date")
    cols.insert(1, "NX")
    df_foreignTrade.columns = cols

    for i in countries.values():
        df_foreignTrade[i+'_NX'] = df_foreignTrade[i+'_EX'] + df_foreignTrade[i+'_IM']

    # Delete unnecessary fields
    df_foreignTrade = df_foreignTrade[df_foreignTrade.columns[0:2].append(df_foreignTrade.columns[20:29])]

    # Fill Nones
    df_foreignTrade = df_foreignTrade.fillna(0)

    df_foreignTrade = df_foreignTrade.groupby('date').mean().reset_index()

    return df_foreignTrade


def import_priceData(path = 'data/strompreise/'):

    files = glob.glob(path + "DE_Großhandelspreise*.csv")


    for i in range(len(files)):
        if i == 0:
            df_price = pd.read_csv(files[i],
                        sep=";",
                        decimal=r",",
                        thousands=r".",
                        converters = {num: numberparse for num in np.arange(2, 22)},
                        parse_dates=['Datum'],
                        date_parser = lambda x: pd.datetime.strptime(x, '%d.%m.%Y'))
        else:
            df_price.append(pd.read_csv(files[i],
                                sep=";",
                                decimal=r",",
                                thousands=r".",
                                converters = {num: numberparse for num in np.arange(2, 22)},
                                parse_dates=['Datum'],
                                date_parser = lambda x: pd.datetime.strptime(x, '%d.%m.%Y')))

    df_price = df_price.groupby('Datum').mean().reset_index()

    df_price.rename(columns={'Datum':'date'}, inplace=True)

    return df_price


def import_consumptionData(path = 'data/Stromverbrauch_real/'):

    files = glob.glob(path + "DE_Realisierter Stromverbrauch*.csv")


    for i in range(len(files)):
        if i == 0:
            df_consumption = pd.read_csv(files[i],
                        sep=";",
                        decimal=r",",
                        thousands=r".",
                        converters = {num: numberparse for num in np.arange(2, 22)},
                        parse_dates=['Datum'],
                        date_parser = lambda x: pd.datetime.strptime(x, '%d.%m.%Y'))
        else:
            df_consumption.append(pd.read_csv(files[i],
                                sep=";",
                                decimal=r",",
                                thousands=r".",
                                converters = {num: numberparse for num in np.arange(2, 22)},
                                parse_dates=['Datum'],
                                date_parser = lambda x: pd.datetime.strptime(x, '%d.%m.%Y')))

    df_consumption = preprocessing_stromverbrauch(df_consumption, 1)

    df_consumption.reset_index().iloc[:, 0:2]
    df_consumption.rename(columns={'Datum':'date'}, inplace=True)

    return df_consumption








