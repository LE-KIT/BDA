#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import glob
numberparse = lambda x: pd.np.float(x.replace(".", "").replace(",",".")) if x!="-" else np.nan

def import_crossborderData_CZ(path = 'data/CZECH REPUBLIC/', file = 'crossborder_cz.csv'):

    df_crossborder = pd.read_csv(path+file,
                                usecols=[0, 9, 10],
                                parse_dates=['Date'],
                                date_parser=lambda x: pd.datetime.strptime(x, '%d.%m.%Y %H:%M'),
                                header=2)
    df_crossborder.columns = ['date', 'CZ_DE_sum_Trade_MW_actual', 'CZ_DE_sum_Trade_MW_planned']
    df_crossborder['CZ_DE_sum_Trade_MW_actual'] = df_crossborder['CZ_DE_sum_Trade_MW_actual'].apply(lambda x: x.replace(',', '.')).astype(float)

    # Average values per day
    df_crossborder = df_crossborder.set_index('date').resample('D').mean().reset_index()

    return df_crossborder


def import_productionData_CZ(path='data/CZECH REPUBLIC/', file='planned_gen_cz.csv'):

    df_production_CZ = pd.read_csv(path + file,
                                    usecols=[0,1],
                                    parse_dates=['Date'],
                                    header=2)

    df_production_CZ.columns = ['date', 'CZ_production_MW_planned']
    df_production_CZ['CZ_production_MW_planned'] = df_production_CZ['CZ_production_MW_planned'].apply(lambda x: x.replace(',', '.')).astype(float)
    return df_production_CZ





def import_productionData_FR(path = 'data/FRANCE/'):

    files = glob.glob(path + "planned_gen_*.csv")

    for i in range(len(files)):
        if i == 0:
            df_production_FR = pd.read_csv(files[i],
                                            parse_dates=['Date de production'],
                                            date_parser=lambda x: pd.datetime.strptime(x, '%d/%m/%Y'),
                                            usecols=[0, 2, 3],
                                            #error_bad_lines=False,
                                            skipfooter=1)
        else:
            df_production_FR.append(pd.read_csv(files[i],
                                            parse_dates=['Date de production'],
                                            date_parser=lambda x: pd.datetime.strptime(x, '%d/%m/%Y'),
                                            usecols=[0, 2, 3],
                                            #error_bad_lines=False,
                                            skipfooter=1))

    df_production_FR.columns = ['date', 'FR_production_MW_planned', 'FR_production_MW_error']

    # Use actual production instead of error
    df_production_FR['FR_production_MW_actual'] = df_production_FR['FR_production_MW_planned'] + df_production_FR['FR_production_MW_error']

    # Average values per day
    df_production_FR = df_production_FR.groupby('date').mean().reset_index().drop(columns=['FR_production_MW_error'])

    return df_production_FR


def import_productionData_DE_planned(path = 'data/Produktion und Infrastruktur/'):

    files = glob.glob(path + "Prognostizierte_Erzeugung*.csv")

    for i in range(len(files)):
        if i == 0:
            df_production_DE = pd.read_csv(files[i],
                                    sep=';',
                                    decimal=',',
                                    thousands='.',
                                    usecols=[0,2],
                                    parse_dates=['Datum'],
                                    na_values='-')
        else:
            df_production_DE.append(pd.read_csv(files[i],
                                        sep=';',
                                        decimal=',',
                                        thousands='.',
                                        usecols=[0,2],
                                        parse_dates=['Datum'],
                                        na_values='-'))

    df_production_DE.columns = ['date', 'DE_production_MW_planned']

    df_production_DE = df_production_DE.groupby('date').sum().reset_index()

    return df_production_DE


def import_productionData_DE_actual(path = 'data/Produktion und Infrastruktur/'):

    files = glob.glob(path + "Realisierte_Erzeugung*.csv")

    for i in range(len(files)):
        if i == 0:
            df_production_DE = pd.read_csv(files[i],
                                    sep=';',
                                    decimal=',',
                                    thousands='.',
                                    parse_dates=['Datum'],
                                    na_values='-')
        else:
            df_production_DE.append(pd.read_csv(files[i],
                                        sep=';',
                                        decimal=',',
                                        thousands='.',
                                        parse_dates=['Datum'],
                                        na_values='-'))

    df_production_DE.rename(columns={'Datum':'date'}, inplace=True)
    df_production_DE['DE_production_MW_actual'] = df_production_DE.iloc[:, 2:].sum(axis=1)

    df_production_DE = df_production_DE[['date', 'DE_production_MW_actual']].groupby('date').sum().reset_index()


    return df_production_DE




def import_productionData():

    productionData = import_productionData_FR().merge(right=import_crossborderData_CZ(),
                                                      on='date',
                                                      how='outer').merge(
                                                            right=import_productionData_CZ(),
                                                            on='date',
                                                            how='outer').merge(right=import_productionData_DE_planned(),
                                                                on='date',
                                                                how='outer').merge(right=import_productionData_DE_actual(),
                                                                        on='date',
                                                                        how='outer')
                                                                                                      

    return productionData


