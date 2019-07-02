import pandas as pd
import numpy as np
import re
import os


def _create_datetime(row):
    date = row.Datum.strftime("%Y-%m-%d") + " " + row.Uhrzeit
    return date


def prepare_stromfluesse(path_to_data_storage):

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

    for key, value in countries.items():
        expo = value + "_EX"
        impo = value + "_IM"
        df[value] = df[expo] + df[impo]
        df.drop([expo, impo], axis=1, inplace=True)

    return df


def prepare_strompreise(path_to_data_storage, aggregate=True):

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
            df_price = df_price.append(df2)
        else:
            df_price = pd.read_csv(
                PATH,
                sep=r";",
                decimal=r",",
                thousands=r".",
                converters=convert_thousand,
                parse_dates=["Datum"],
                date_parser=dateparse,
            )

    df_price["Date"] = df_price.apply(lambda row: _create_datetime(row), axis=1)
    df_price["Date"] = pd.to_datetime(df_price.Date, format="%Y-%m-%d %H:%M")
    df_price = df_price.sort_values("Date").reset_index(drop=True)

    cols = list(df_price)
    cols.insert(0, cols.pop(cols.index("Date")))
    df_price = df_price.loc[:, cols]
    df_price.drop(["Uhrzeit"], axis=1, inplace=True)

    delete_currency = r"(.*?)\["
    new_columns = [
        "price_" + re.search(delete_currency, col).group(1).lower()
        for col in df_price.columns[2::]
    ]
    new_columns.insert(0, "Date")
    new_columns.insert(1, "Tag")
    df_price.columns = new_columns

    df_price.fillna(df_price.mean(), inplace=True)

    df_price_aggregated = pd.DataFrame(columns=df_price.columns[2::])

    ind = 0
    for name, df in df_price.groupby("Tag"):
        mean_price_day = df.iloc[:, 2::].mean()
        df_price_aggregated = df_price_aggregated.append(
            mean_price_day, ignore_index=True
        )

    df_price_aggregated.columns = [
        "daily_" + col for col in df_price_aggregated.columns
    ]

    df_price_aggregated["Tag"] = df_price.Tag.unique()

    cols = list(df_price_aggregated)
    cols.insert(0, cols.pop(cols.index("Tag")))
    df_price_aggregated = df_price_aggregated.loc[:, cols]

    if aggregate == True:
        return df_price_aggregated
    else:
        return df_price


def prepare_konsum_ger(path_to_data_storage):
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
            df_consumption = df_consumption.append(df2)
        else:
            df_consumption = pd.read_csv(
                PATH,
                sep=r";",
                decimal=r",",
                thousands=r".",
                converters=convert_thousand,
                parse_dates=["Datum"],
                date_parser=dateparse,
            )

    df_consumption = df_consumption.groupby("Datum").sum()
    df_consumption.columns = ["daily_consumption_ger"]

    df_consumption.dropna(inplace=True)
    df_consumption["Tag"] = df_consumption.index

    cols = list(df_consumption)
    cols.insert(0, cols.pop(cols.index("Tag")))
    df_consumption = df_consumption.loc[:, cols]

    df_consumption.reset_index(drop=True, inplace=True)

    return df_consumption


def enrich_daily_information(df_price, df_consumption):

    df_daily_information = df_consumption.merge(df_price, on="Tag")

    cols = ["prev_day" + col[5::] for col in df_daily_information.columns[1::]]
    cols.insert(0,'Tag')
    df_daily_information.columns = cols
    # Shift
    day_shift = 1
    df_daily_information.iloc[:, 1::] = df_daily_information.iloc[:, 1::].shift(
        day_shift
    )
    df_daily_information.dropna(inplace=True)

    # Zusätliche Tagesinformationen
    df_daily_information["time_diff_days"] = (
        df_daily_information["Tag"] - pd.Timestamp(df_daily_information["Tag"].min())
    ).dt.days

    df_daily_information["day_of_week"] = df_daily_information["Tag"].dt.day_name()
    df_daily_information = pd.concat(
        [df_daily_information, pd.get_dummies(df_daily_information["day_of_week"])],
        axis=1,
    )
    df_daily_information.drop(["day_of_week"], inplace=True, axis=1)

    df_daily_information["month"] = df_daily_information["Tag"].dt.month_name()
    df_daily_information = pd.concat(
        [df_daily_information, pd.get_dummies(df_daily_information["month"])], axis=1
    )
    df_daily_information.drop(["month"], inplace=True, axis=1)

    return df_daily_information
