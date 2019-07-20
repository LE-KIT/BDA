import pandas as pd
import numpy as np
import os
import re

from datetime import timedelta
from sklearn.preprocessing import StandardScaler, MinMaxScaler,RobustScaler
from bda.bda_utilities import find_missing_rows
from scripts.preprocessing_master import import_masterDataFrame

def _create_datetime(row):
    date = row.Datum.strftime("%Y-%m-%d") + " " + row.Uhrzeit
    return date

def prepare_stromfluesse(path_to_data_storage,aggregate=True):

    dateparse = lambda x: pd.datetime.strptime(x, "%d.%m.%Y")
    numberparse = (
        lambda x: pd.np.float(x.replace(".", "").replace(",", "."))
        if x != "-"
        else np.nan
    )
    convert_thousand = {num: numberparse for num in np.arange(2, 22)}

    import_files = os.listdir(path_to_data_storage)

    for idx, file in enumerate(import_files):
        print("Import File: {} ".format(file))
        PATH = path_to_data_storage + file
        if idx > 0:
            df2 = pd.read_csv(
                PATH,
                sep=r";",
                decimal=r",",
                thousands=r".",
                converters=convert_thousand,
                parse_dates=["Datum"],
                date_parser=dateparse,
            )
            df = df.append(df2)
        else:
            df = pd.read_csv(
                PATH,
                sep=r";",
                decimal=r",",
                thousands=r".",
                converters=convert_thousand,
                parse_dates=["Datum"],
                date_parser=dateparse,
            )

    df["Date"] = df.apply(lambda row: _create_datetime(row), axis=1)
    df["Date"] = pd.to_datetime(df.Date, format="%Y-%m-%d %H:%M")
    df = df.sort_values("Date").reset_index(drop=True)

    cols = list(df)
    cols.insert(0, cols.pop(cols.index("Date")))
    df = df.loc[:, cols]
    df.drop(["Uhrzeit"], axis=1, inplace=True)

    type_pattern = r"\((.*?)\)"
    country_pattern = r"(.*?) "
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

    new_columns = [
        countries.get(re.search(country_pattern, col).group(1))
        + "_"
        + types.get(re.search(type_pattern, col).group(1))
        for col in df.columns[3::]
    ]
    new_columns.insert(0, "Date")
    new_columns.insert(1, "Tag")
    new_columns.insert(2, "NX")
    df.columns = new_columns

    df["NX"] = df.loc[:, "NL_EX":"PL_IM"].sum(axis=1)

    df = df.fillna(0)

    if aggregate:
        for key, value in countries.items():
            expo = value + "_EX"
            impo = value + "_IM"
            df[value] = df[expo] + df[impo]
            df.drop([expo, impo], axis=1, inplace=True)

    
    df = df.groupby('Date').agg('mean').reset_index(drop=False)

    df['Tag'] = df['Date'].dt.date
    df['Tag'] = pd.to_datetime(df['Tag'])


    idx = find_missing_rows(df.set_index('Date'))

    for index in idx:
        row = df.iloc[index-1].copy()
        row.Date = row.Date + timedelta(hours=1)
        df = df.append(row)
        
    df = df.sort_values('Date').reset_index(drop=True)

    return df


def import_model_data(path_stromfluesse, path_additional_data,aggregate=False):
    df_stromfluesse = prepare_stromfluesse("data/Stromfluss/",aggregate)
    df_stromfluesse['Date'] =  pd.to_datetime(df_stromfluesse['Date'])

    df_xData = import_masterDataFrame('/Users/zcjr/Documents/Uni/Master BDA/')
    df_xData['date'] =  pd.to_datetime(df_xData['date'])

    df = df_stromfluesse.merge(df_xData,how="left",left_on='Date',right_on="date")
    df.drop('date',1,inplace=True)
    
    return df

def extract_daily_informatin(df):
    df['Tag'] =  pd.to_datetime(df['Tag'])

    df['weekday'] = df["Tag"].dt.day_name()
    df['month'] = df["Tag"].dt.month_name()
    df["time_diff_days"] = (
            df["Tag"] - pd.Timestamp(df["Tag"].min())
        ).dt.days

    df = pd.concat(
            [df, pd.get_dummies(df[['weekday','month','time_diff_days']])], axis=1
        )
    df = df.drop(['weekday','month','time_diff_days'],axis=1)
    return df

def remove_outlier(df,countries):
    for country in countries:
        iqr = df[country].quantile(0.75) - df[country].quantile(0.25)
        mean =  df[country].median()
        df[country] = df[country].apply(lambda row: mean+3*iqr if row>mean+3*iqr else mean-3*iqr if row< mean-3*iqr else row)
    return df

def scale_data(df,countries,add_x):
    scaler = MinMaxScaler()
    df[countries] = scaler.fit_transform(df[countries])

    df[add_x] = df[add_x].fillna(0)
    add_scaler = MinMaxScaler()
    df[add_x] = add_scaler.fit_transform(df[add_x])

    return df,scaler

def aggregate_nx(df,countries,rolling_days=1):
    
    df_master = pd.DataFrame(index=np.arange(0,df.Tag.nunique()))
    df_master['Tag'] = None
    df_master['NX_per_country'] = None

    ind = 0
    for tag,group in df.groupby('Tag'):
        df_master.loc[ind,'Tag'] =  tag
        df_master.loc[ind,'NX_per_country'] =  group.loc[:,countries].values
        ind +=1
        
    for day in np.arange(1,rolling_days):
        name = 'NX_per_country_shifted_' + str(day)
        df_master[name] = df_master['NX_per_country'].shift(day)
    
    df_master.dropna(inplace=True)


    # Aggregate NX per day --> gestern -> heute
    names = ['NX_per_country_shifted_' + str(day) for day in reversed(np.arange(1,rolling_days))]
    names = names + ['NX_per_country']
    df_master['NX_per_country_aggregated'] = df_master.loc[:,names].apply(lambda row: np.concatenate(row), axis=1)


    # Shift von gestern/heute --> gestern/vorgestern 
    df_master['NX_per_country_shifted'] = df_master['NX_per_country_aggregated'].shift(1)


    df_master.dropna(inplace=True)
    df_master['Tag'] =  pd.to_datetime(df_master['Tag'], format='%Y-%m-%d')
    df_master = df_master[['Tag','NX_per_country','NX_per_country_shifted']]

    return df_master

def aggregate_additional_input_data(df,additional_input_columns,rolling_days=1):
    df_master = pd.DataFrame(index=np.arange(0,df.Tag.nunique()))
    df_master['Tag'] = None
    df_master['Additional_X'] = None

    ind = 0
    for tag,group in df.groupby('Tag'):
        df_master.loc[ind,'Tag'] =  tag
        df_master.loc[ind,'Additional_X'] =  group.loc[:,additional_input_columns].values
        ind +=1
        
    for day in np.arange(1,rolling_days):
        name = 'Additional_X_shifted_' + str(day)
        df_master[name] = df_master['Additional_X'].shift(day)

    df_master.dropna(inplace=True)

    # Aggregate NX per day --> gestern -> heute
    names = ['Additional_X_shifted_' + str(day) for day in reversed(np.arange(1,rolling_days))]
    names = names + ['Additional_X']
    df_master['Additional_X_aggregated'] = df_master.loc[:,names].apply(lambda row: np.concatenate(row), axis=1)


    df_master.dropna(inplace=True)
    df_master['Tag'] =  pd.to_datetime(df_master['Tag'], format='%Y-%m-%d')
    df_master = df_master[['Tag','Additional_X_aggregated']]

    return df_master


def reformat_data_for_training(trainX,trainY,testX,testY):
    trainY = np.squeeze(np.array(trainY.tolist()),1)
    trainX = np.array(trainX.tolist())
    testY = np.squeeze(np.array(testY.tolist()),1)
    testX = np.array(testX.tolist())
    return trainX,trainY,testX,testY