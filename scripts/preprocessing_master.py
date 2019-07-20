#!/usr/bin/env python
# coding: utf-8

from scripts.preprocessing_plannedProduction import *
from scripts.preprocessing_price_consumption import *
from scripts.preprocessing_weatherdata import *
from bda.bda_utilities import find_missing_rows

from datetime import timedelta

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



    df_master = pd.read_csv('data/master_df.csv',sep=";",decimal=",")

    df_master.columns = [col.replace(" ","").lower() for col in df_master.columns]
    df_master['date'] =  pd.to_datetime(df_master['date'])
    df_master = df_master.loc[(df_master.date>='2015-04-01 00:00:00') & (df_master.date<='2019-06-01 23:00:00')]
    df_master = df_master.sort_values('date').reset_index(drop=True)

    nx_cols = [col for col in df_master.columns if '_nx' in col]
    df_master = df_master.drop(nx_cols,axis=1)

    df_master = df_master.groupby('date').agg('mean').reset_index(drop=False)
    df_master['Tag'] = df_master['date'].dt.date
    df_master['Tag'] = pd.to_datetime(df_master['Tag'])


    # Find Missing Rows and Forward Fill
    idx = find_missing_rows(df_master.set_index('date'))

    for index in idx:
        row = df_master.iloc[index-1].copy()
        row.date = row.date + timedelta(hours=1)
        df_master = df_master.append(row)
        
    df_master = df_master.sort_values('date').reset_index(drop=True)

    # Fill NANs
    df_master = df_master.fillna(0)

    actual_cols = [col for col in df_master.columns if 'actual' in col]
    price_cols = [col for col in df_master.columns if 'price' in col]
    shift_cols = actual_cols + price_cols + ['de_consumption_mw']

    df_master[shift_cols] = df_master[shift_cols].shift(1)
    df_master = df_master.drop(0,axis=0)

    df_master.drop('Tag',axis=1,inplace=True)
    
    return df_master
