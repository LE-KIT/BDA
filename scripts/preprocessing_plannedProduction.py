#!/usr/bin/env python
# coding: utf-8

import glob
import re

import pandas as pd

from scripts.datetimeManipulation import make_hourly


def import_crossborderData_CZ(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/', path='data/CZECH REPUBLIC/',
                              file='crossborder_cz.csv'):
    print('Starting import_crossborderData_CZ')

    df_crossborder_CZ = pd.read_csv(projectPath + path + file,
                                    usecols=[0, 9, 10],
                                    parse_dates=['Date'],
                                    date_parser=lambda x: pd.datetime.strptime(x, '%d.%m.%Y %H:%M'),
                                    header=2)
    df_crossborder_CZ.columns = ['date', 'CZ_DE_sum_Trade_MW_actual', 'CZ_DE_sum_Trade_MW_planned']
    df_crossborder_CZ['CZ_DE_sum_Trade_MW_actual'] = df_crossborder_CZ['CZ_DE_sum_Trade_MW_actual'].apply(
        lambda x: x.replace(',', '.')).astype(float)

    # MW --> MWh, no change necessary

    # Time format : 24-01-2018 12:34
    # df_crossborder_CZ['date'] = df_crossborder_CZ['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    df_crossborder_CZ = df_crossborder_CZ[
        df_crossborder_CZ['date'] >= pd.to_datetime('01-06-2017 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    df_crossborder_CZ['Dummy'] = 1

    if len(df_crossborder_CZ.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        df_crossborder_CZ = df_crossborder_CZ.groupby('date').mean().reset_index()

    df_crossborder_CZ.drop('Dummy', axis=1, inplace=True)

    print('Finished import_crossborderData_CZ')

    return df_crossborder_CZ


def import_crossborderData_DE(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/',
                              path='data/Produktion und Infrastruktur/'):
    print('Starting import_crossborderData_DE')

    files = glob.glob(projectPath + path + "Kommerzieller_Au_enhandel*.csv")

    for i in range(len(files)):
        if i == 0:
            df_crossborder_DE = pd.read_csv(files[i],
                                            sep=';',
                                            decimal=',',
                                            thousands='.',
                                            parse_dates=[['Datum', 'Uhrzeit']],
                                            na_values='-')
        else:
            df_crossborder_DE = df_crossborder_DE.append(pd.read_csv(files[i],
                                                                     sep=';',
                                                                     decimal=',',
                                                                     thousands='.',
                                                                     parse_dates=[['Datum', 'Uhrzeit']],
                                                                     na_values='-'))

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
            for col in df_crossborder_DE.columns[2::]]
    cols.insert(0, "date")
    cols.insert(1, "NX")
    df_crossborder_DE.columns = cols
    for i in countries.values():
        df_crossborder_DE[i + '_NX'] = df_crossborder_DE[i + '_EX'] + df_crossborder_DE[i + '_IM']
    # Delete unnecessary fields
    df_crossborder_DE = df_crossborder_DE[df_crossborder_DE.columns[0:2].append(df_crossborder_DE.columns[20:29])]

    # Fill NX field
    df_crossborder_DE['NX'] = df_crossborder_DE.iloc[:, 2:].sum(axis=1)

    # Fill NaN
    df_crossborder_DE.fillna(0, inplace=True)

    # Time format : 24-01-2018 12:34
    # df_crossborder_DE['date'] = df_crossborder_DE['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    df_crossborder_DE = df_crossborder_DE[
        df_crossborder_DE['date'] >= pd.to_datetime('01-06-2017 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    df_crossborder_DE['Dummy'] = 1
    if len(df_crossborder_DE.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        df_crossborder_DE = df_crossborder_DE.groupby('date').mean().reset_index()
    df_crossborder_DE.drop('Dummy', axis=1, inplace=True)

    print('Finished import_crossborderData_DE')

    return df_crossborder_DE


def import_productionData_CZ(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/', path='data/CZECH REPUBLIC/',
                             file='planned_gen_cz.csv'):
    print('Starting import_productionData_CZ')

    df_production_CZ = pd.read_csv(projectPath + path + file,
                                   usecols=[0, 1],
                                   parse_dates=['Date'],
                                   header=2)

    df_production_CZ.columns = ['date', 'CZ_production_MW_planned']
    df_production_CZ['CZ_production_MW_planned'] = df_production_CZ['CZ_production_MW_planned'].apply(
        lambda x: x.replace(',', '.')).astype(float)

    # Make hourly
    df_production_CZ = make_hourly(df_production_CZ)

    # [MW] --> to get MWh, we need to divide production by 24 /per hour
    df_production_CZ['CZ_production_MW_planned'] = df_production_CZ['CZ_production_MW_planned'] / 24

    # Time format : 24-01-2018 12:34
    # df_production_CZ['date'] = df_production_CZ['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    df_production_CZ = df_production_CZ[
        df_production_CZ['date'] >= pd.to_datetime('01-06-2017 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    df_production_CZ['Dummy'] = 1

    if len(df_production_CZ.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        df_production_CZ = df_production_CZ.groupby('date').mean().reset_index()

    df_production_CZ.drop('Dummy', axis=1, inplace=True)

    print('Finished import_productionData_CZ')

    return df_production_CZ


def import_productionData_FR(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/', path='data/FRANCE/'):
    print('Starting import_productionData_FR')

    files = glob.glob(projectPath + path + "planned_gen_*.csv")

    for i in range(len(files)):
        if i == 0:
            df_production_FR = pd.read_csv(files[i],
                                           parse_dates=[['Date de production', 'Heures']],
                                           # error_bad_lines=False,
                                           skipfooter=1)
        else:
            df_production_FR = df_production_FR.append(pd.read_csv(files[i],
                                                                   parse_dates=[['Date de production', 'Heures']],
                                                                   # error_bad_lines=False,
                                                                   skipfooter=1))

    df_production_FR.columns = ['date', 'FR_production_MW_planned', 'FR_production_MW_error']

    # Use actual production instead of error
    df_production_FR['FR_production_MW_actual'] = df_production_FR['FR_production_MW_planned'] + df_production_FR[
        'FR_production_MW_error']

    # Average values per hour # [MW] --> SUM
    df_production_FR = df_production_FR.set_index('date').resample('H').sum().reset_index()

    # Time format : 24-01-2018 12:34
    # df_production_FR['date'] = df_production_FR['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    df_production_FR = df_production_FR[
        df_production_FR['date'] >= pd.to_datetime('01-06-2017 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    df_production_FR['Dummy'] = 1

    if len(df_production_FR.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        df_production_FR = df_production_FR.groupby('date').mean().reset_index()

    df_production_FR.drop('Dummy', axis=1, inplace=True)

    print('Finished import_productionData_CZ')

    return df_production_FR


def import_productionData_DE_planned(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/',
                                     path='data/Produktion und Infrastruktur/'):
    print('Starting import_productionData_DE_planned')

    files = glob.glob(projectPath + path + "Prognostizierte_Erzeugung*.csv")

    for i in range(len(files)):
        if i == 0:
            df_production_DE = pd.read_csv(files[i],
                                           sep=';',
                                           decimal=',',
                                           thousands='.',
                                           usecols=[0, 1, 2],
                                           parse_dates=[['Datum', 'Uhrzeit']],
                                           na_values='-')
        else:
            df_production_DE = df_production_DE.append(pd.read_csv(files[i],
                                                                   sep=';',
                                                                   decimal=',',
                                                                   thousands='.',
                                                                   usecols=[0, 1, 2],
                                                                   parse_dates=[['Datum', 'Uhrzeit']],
                                                                   na_values='-'))

    df_production_DE.columns = ['date', 'DE_production_MW_planned']

    # Sum of the values per hour # [MWh] --> MEAN VS SUM --> SUM, due to weird calculation in rawdata
    df_production_DE = df_production_DE.set_index('date').resample('H').sum().reset_index()

    # Time format : 24-01-2018 12:34
    # df_production_DE['date'] = df_production_DE['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    df_production_DE = df_production_DE[
        df_production_DE['date'] >= pd.to_datetime('01-06-2017 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    df_production_DE['Dummy'] = 1

    if len(df_production_DE.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        df_production_DE = df_production_DE.groupby('date').mean().reset_index()

    df_production_DE.drop('Dummy', axis=1, inplace=True)

    print('Finished import_productionData_DE_planned')

    return df_production_DE


def import_productionData_DE_actual(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/',
                                    path='data/Produktion und Infrastruktur/'):
    print('Starting import_productionData_DE_actual')

    files = glob.glob(projectPath + path + "Realisierte_Erzeugung*.csv")

    for i in range(len(files)):
        if i == 0:
            df_production_DE = pd.read_csv(files[i],
                                           sep=';',
                                           decimal=',',
                                           thousands='.',
                                           parse_dates=[['Datum', 'Uhrzeit']],
                                           na_values='-')
        else:
            df_production_DE = df_production_DE.append(pd.read_csv(files[i],
                                                                   sep=';',
                                                                   decimal=',',
                                                                   thousands='.',
                                                                   parse_dates=[['Datum', 'Uhrzeit']],
                                                                   na_values='-'))

    df_production_DE.rename(columns={'Datum_Uhrzeit': 'date'}, inplace=True)
    df_production_DE['DE_production_MW_actual'] = df_production_DE.iloc[:, 2:].sum(axis=1)

    # Sum of the values per hour # [MWh] --> MEAN VS SUM --> SUM, due to weird calculation in rawdata
    df_production_DE = df_production_DE.set_index('date').resample('H').sum().reset_index()

    # Time format : 24-01-2018 12:34
    # df_production_DE['date'] = df_production_DE['date'].apply(lambda x: x.strftime('%d-%m-%Y %H:%M'))

    # Delete unnecessary rows
    df_production_DE = df_production_DE[
        df_production_DE['date'] >= pd.to_datetime('01-06-2017 00:00:00', format='%d-%m-%Y %H:%M:%S')]

    # Handle multiple datetime rows
    df_production_DE['Dummy'] = 1

    if len(df_production_DE.groupby('date').count()['Dummy'].where(lambda x: x != 1).dropna()) > 0:
        df_production_DE = df_production_DE.groupby('date').mean().reset_index()

    df_production_DE.drop('Dummy', axis=1, inplace=True)

    print('Finished import_productionData_DE_actual')

    return df_production_DE


def import_productionData(projectPath='/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/'):
    print('Starting import_productionData')

    productionData = import_productionData_FR().merge(right=import_crossborderData_CZ(projectPath),
                                                      on='date',
                                                      how='outer').merge(
        right=import_productionData_CZ(projectPath),
        on='date',
        how='outer').merge(right=import_productionData_DE_planned(projectPath),
                           on='date',
                           how='outer').merge(right=import_productionData_DE_actual(projectPath),
                                              on='date',
                                              how='outer').merge(right=import_crossborderData_DE(projectPath),
                                                                 on='date',
                                                                 how='outer')

    print('Finished import_productionData')

    return productionData
