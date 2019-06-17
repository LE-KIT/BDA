"""Summary
"""
import pandas as pd
import re


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


def preprocessing_stromfluss(df):
    """Preprocessing für Stromfluss Datansatz von Smard
    
    Parameters
    ----------
    df : pd.DataFrame
        Stromfluss Datensatz von SMARD eingelesen und unbearbeitet
    
    Returns
    -------
    pd.DataFrame
        Stromfluss Datensatz von SMARD aufbereitet zur weiteren Verwendung
    """

    # Time Formatting
    df["Date"] = df.apply(lambda row: _create_datetime(row), axis=1)
    df["Date"] = pd.to_datetime(df.Date, format="%Y-%m-%d %H:%M")
    df = df.sort_values("Date").reset_index(drop=True)
    cols = list(df)
    cols.insert(0, cols.pop(cols.index("Date")))
    df = df.loc[:, cols]
    df.drop(["Uhrzeit"], axis=1, inplace=True)

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

    # Netto Exporte Füllen
    export_columns = [col for col in df.columns if col[-2::] == "EX"]
    import_columns = [col for col in df.columns if col[-2::] == "IM"]
    df["NX"] = df.loc[:, "NL_EX":"PL_IM"].sum(axis=1)

    # Fill Nones
    df = df.fillna(0)

    return df


def feature_exrraction_stromfluss(df,hour_shift=24):
    """Summary
    
    Parameters
    ----------
    df : pd.DataFrame
    hour_shift : int, optional
        Anzahl Shift Stunden

    Returns
    -------
    pd.DataFrame
    """
    # Calculate NX for each country
    prev_day_nx = df.iloc[:,2::].groupby(lambda x: x.split('_')[0], axis=1).sum()
    prev_day_nx['Date'] = df['Date']
    prev_day_nx.loc[:,prev_day_nx.columns!='Date'] = prev_day_nx.loc[:,prev_day_nx.columns!='Date'].shift(hour_shift)
    prev_day_nx.dropna(inplace=True)

    # Rename columns
    columns = [col +"_nx_prev_day" for col in prev_day_nx.loc[:,prev_day_nx.columns!='Date'].columns]
    columns.insert(len(columns)+1,'Date')
    prev_day_nx.columns = columns

    # Merge with df
    df = pd.merge(df,prev_day_nx,on="Date",how="inner")

    return df

def preprocessing_strompreise(df_price, hour_shift=24, hour_rolling_window=24):
    """Preprocessing für Strompreis Datansatz von Smard
    
    Parameters
    ----------
    df_price : pd.Dataframe
        Stromprei Datensatz vom SMARD unbearbeitet
    hour_shift : int, optional
        Anzahl Shift Stunden
    hour_rolling_window : int, optional
        Größe des stündlichen rolling windows 
    
    Returns
    -------
    pd.DataFrame
        Stromprei Datensatz vom SMARD bearbeitet
    
    """

    # Time Formatting
    df_price["Date"] = df_price.apply(lambda row: _create_datetime(row), axis=1)
    df_price["Date"] = pd.to_datetime(df_price.Date, format="%Y-%m-%d %H:%M")
    df_price = df_price.sort_values("Date").reset_index(drop=True)

    cols = list(df_price)
    cols.insert(0, cols.pop(cols.index("Date")))
    df_price = df_price.loc[:, cols]
    df_price.drop(["Uhrzeit"], axis=1, inplace=True)

    # Rename columns
    delete_currency = r"(.*?)\["
    new_columns = [
        "price_" + re.search(delete_currency, col).group(1).lower()
        for col in df_price.columns[2::]
    ]
    new_columns.insert(0, "Date")
    new_columns.insert(1, "Tag")

    df_price.columns = new_columns

    # Fill na/nones
    df_price.fillna(df_price.mean(), inplace=True)

    # Shift and calculate rolling window
    df_price.iloc[:, 2::] = (
        df_price.iloc[:, 2::].shift(hour_shift).rolling(hour_rolling_window).mean()
    )
    df_price.dropna(inplace=True)
    return df_price


def preprocessing_stromverbrauch(df_consumption, day_shift=1):
    """Preprocessing für Stromverbrauch Datansatz von Smard
    
    Parameters
    ----------
    df_consumption : pd.DataFrame
        Realisierter Stromverbrauch Datensatz für Deutschland vom SMARD unbearbeitet
    day_shift : int, optional
        Description
    day_shift : int, optional
    
    Returns
    -------
    pd.DataFrame
        Realisierter Stromverbrauch Datensatz für Deutschland vom SMARD bearbeitet
    """
    df_consumption = df_consumption.groupby("Datum").sum()

    df_consumption.columns = ["shifted_daily_consumption_ger"]

    day_shift = 1
    df_consumption = df_consumption.shift(day_shift)
    df_consumption.dropna(inplace=True)

    df_consumption["Tag"] = df_consumption.index

    return df_consumption
