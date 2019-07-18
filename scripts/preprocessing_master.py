#!/usr/bin/env python
# coding: utf-8

from scripts.preprocessing_plannedProduction import *
from scripts.preprocessing_price_consumption import *
from scripts.preprocessing_weatherdata import *


def preprocess_Rawdata(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/'):
    df = pd.merge(left=import_weatherData(projectPath),
                  right=import_productionData(projectPath),
                  on='date',
                  how='outer').merge(right=import_price_consumption_Data(projectPath),
                                     on='date',
                                     how='outer')
    df = df.sort_values('date')

    # Delete empty rows of export due to time change --> Rows after 02-06-2019 stay in the df
    df = df.drop(df[(df['date'] < pd.to_datetime('02-06-2019 00:00:00', format='%d-%m-%Y %H:%M:%S')) & (
        df.iloc[:, 33:43].isnull().any(axis=1))].index).reset_index(drop=True)

    # Export to csv
    df.to_csv(projectPath + 'data/master_df.csv', sep=';', decimal=',', index=False)

    return df


def import_masterDataFrame(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/', path='data/master_df.csv'):
    df_master = pd.read_csv(projectPath + path, sep=';', decimal=',', parse_dates=['date'])

    return df_master
