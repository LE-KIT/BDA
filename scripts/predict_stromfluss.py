import pandas as pd
import numpy as np
import re
import os
from datetime import datetime, timedelta
from sklearn import linear_model as lm
pd.options.mode.chained_assignment = None

def _create_datetime(row):
    """Helper Function

    Parameters
    ----------
    row : TYPE
        Description
    row : pd.Series

    Returns
    -------
    pd.Datetime
    """
    date = row.Datum.strftime("%Y-%m-%d") + " " + row.Uhrzeit
    return date

def read_data_stromfluss(data_dir):
    """Read data from directory, get rid of '-' and NaN values, export a dataframe"""

    dateparse = lambda x: pd.datetime.strptime(x, '%d.%m.%Y')
    numberparse = lambda x: pd.np.float(x.replace(".", "").replace(",", ".")) if x != "-" else np.nan
    convert_thousand = {num: numberparse for num in np.arange(2, 22)}

    import_files = os.listdir(data_dir)

    for i in range(len(import_files)):
        if i == 0:
            df = pd.read_csv(os.path.join(data_dir, import_files[i]), sep=';', decimal=',', thousands='.',
                             parse_dates=['Datum'], date_parser=dateparse, converters=convert_thousand)
        else:
            df = df.append(pd.read_csv(os.path.join(data_dir, import_files[i]), sep=';', decimal=',', thousands='.',
                                       parse_dates=['Datum'], date_parser=dateparse, converters=convert_thousand))

    df.replace('-', 0, inplace=True)
    df.fillna(0, inplace=True)

    return df

def preprocessing_stromfluss(df, basic = False):
    """Preprocessing für stromfluss Datansatz von Smard

    Parameters
    ----------
    df : pd.DataFrame
        stromfluss Datensatz von SMARD eingelesen und unbearbeitet

    Returns
    -------
    pd.DataFrame
        stromfluss Datensatz von SMARD aufbereitet zur weiteren Verwendung
    """

    # Time Formatting
    df["Date"] = df.apply(lambda row: _create_datetime(row), axis=1)
    df["Date"] = pd.to_datetime(df.Date, format="%Y-%m-%d %H:%M")
    df = df.sort_values("Date")

    cols = list(df)
    cols.insert(0, cols.pop(cols.index("Date")))
    df = df.loc[:, cols]

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

    df.columns = list(df.columns[0:3]) + ['NX'] + [countries.get(re.search(country_pattern, col).group(1))+ "_"+ types.get(re.search(type_pattern, col).group(1)) for col in df.columns[4::]]

    # Netto Export
    df["NX"] = df.iloc[:, 4:].sum(axis = 1)
    
    #Drop columns
    df.drop(df.columns[1:3], axis=1, inplace = True)

    #Export only Datetime and NX for basic analysis
    if basic:
        df = df[['Date', 'NX']]

    return df


def predict_validate(model, df, dateList):
	#Add fields for prediction and absolute error
    df['NX P'] = np.nan
    df['AE'] = np.nan
    
    if model == 'lm':
        for date in dateList:
        	#Predict for dates in dateList
            df = predict_lm(df, date)
            
    else:
        print('Another method')
        #TBD
    
    return df

def predict_lm(df, date):
    #Train test split
    X = df[df['Date'] < date]
    Y = df[(df['Date'] >= date) & (df['Date'] < date + timedelta(days=1))]
    
    #Train & test
    tmp = lm.LinearRegression().fit(X[['Year', 'Month', 'Day', 'Hour']], X['NX']).predict(Y[['Year', 'Month', 'Day', 'Hour']])
    Y.at[:,'NX P'] = tmp
    #Average error
    Y.at[:,'AE'] = (Y['NX'] - Y['NX P']).apply(lambda x: abs(x))
    
    #Update df with AE information
    df.update(Y)
    
    print('Mean average error for {} is: {}'.format(date, Y['AE'].mean()))
    
    return df
    

#Import data
df = read_data_stromfluss('data/stromfluss')
df = preprocessing_stromfluss(df, True)









