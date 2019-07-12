import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
import os.path

from scripts import predict_stromfluss, preprocessing_weatherdata


def get_training_sets(df: object, training_window_width: int, evaluation_days: list)->list:
    '''
    Provides a list containing pairs of training and validation sets for time series forecasts such as ARIMA. Validation is always done on all hours of a single day.
    :param df: pandas.DataFrame containing the time series data
    :param training_window_width: training window width in days. window width stays fix over validations
    :param evaluation_days: list of days on which to perform validations
    :return: list of evaluation sets
    '''

    evaluation_sets = []

    # Split the dataframe in training and evaluation data
    for day in evaluation_days:
        training_data = df[df['Date'].dt.date.between((day - timedelta(days=training_window_days)).date(), day.date())]
        evaluation_data = df[df['Date'].dt.date == day.date()]

        evaluation_sets.append({"training_data": training_data, "evaluation_data": evaluation_data})

    return evaluation_sets



def walk_farward_validation(evaluation_sets: list, processing_function: object, prediction_target: str,
                            model: object,
                            error_function: object) -> object:
    '''

    :rtype: dictionary
    :param processing_function: preprocessing function for the chosen model in the form of preprocessing_function(dataframe)
    :param prediction_taget: column name of the predicted variable
    :param model: the model that is used in the training. this needs to have a fit(X, y) method
    :param error_function: error function to be used to calculate the overall error, e.g. MAE, MSE, etc.
    :return: dictionary {"model": model name, "predicitons": dataframe, "error": overall error}
    '''

    predictions = pd.DataFrame()

    # For each training set, fit the model and make predictions
    for set in evaluation_sets:
        processed_training_data = processing_function(set['training_data'])
        processed_evaluation_data = processing_function(set['evaluation_data'])

        model.fit(X=processed_training_data['Date'].reshape(-1,1), y=processed_training_data[prediction_target])
        print(model.coef_)
        prediction=pd.DataFrame({'Date': processed_evaluation_data['Date'],
                                 'Prediction': model.predict(X=processed_evaluation_data),
                                 'Actual': processed_evaluation_data[prediction_target]})
        if(len(predictions) > 0):
            predictions.merge(prediction)
        else:
            predictions = prediction

    # Over all training sets: calculate the error given the error_function
    error = error_function(y_true=predictions['Actual'], y_pred=predictions['Prediction'])

    return({"id": model.__class__, "predictions" : predictions, "error": error})


def split_datetime(df):
    '''
    Splits up a datetime column into year, month, weekday, hour and timestamp. Automatically selects the first column that has datetime values.
    :param df: pd.DataFrame with a datetime column.
    :return: pd.DataFrame containing new columns
    '''
    date_key = df.select_dtypes(include=[np.datetime64]).columns[0]
    df['year']=df[date_key].apply(lambda x: x.year)
    df['month']=df[date_key].apply(lambda x: x.month)
    df['weekday']=df[date_key].apply(lambda x: x.isoweekday())
    df['timestamp']=df[date_key].apply(lambda x:(x - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's'))
    df['hour'] = df[date_key].apply(lambda x: x.hour)
    df = df.drop(columns=[date_key])
    return df

def merge_datetime(df):
    '''
    "Merges" split up datetime back together. Returned Dataframe only has one date column.
    :param df: pd.DataFrame with a column named "timestamp" containing a np.datetime64 timestamp.
    :return: pd.DataFrame 
    '''
    df['date']=df['timestamp'].apply(lambda x: (x * np.timedelta64(1,'s') + np.datetime64('1970-01-01T00:00:00Z')))
    df = df.drop(columns = ["year","month","weekday", "timestamp", "hour"])
    return df

def make_hourly(df, freq='1H'):
    '''
    Resamples a Dataframe to contain one entry per hour by padding missing values.
    :param df: pd.DataFrame containing less than one entry per hour, for example one entry per day.
    :param freq: (optional) entry for the frequency, should be higher than in original df
    :return: resampled pd.DataFrame
    '''
    date_key = df.select_dtypes(include=[np.datetime64]).columns[0]
    df = df.set_index(df[date_key])
    df = df.resample(freq).pad()
    df = df.drop(columns=[date_key])
    df = df.reset_index()
    return df

def create_master_df(path_to_data_dir="./data/"):
    '''
    Creates and saves the masterframe
    :param path_to_data_dir:
    :return:
    '''
    df = predict_stromfluss.read_data_stromfluss(path_to_data_dir + 'stromfluss')
    df = predict_stromfluss.preprocessing_stromfluss(df)

    def create_net_columns(df):
        countries = ["NL", "CHE", "DNK", "CZE", "LUX", "SWE", "AUT", "FRA", "PL"]
        for country in countries:
            im_key = country + "_IM"
            ex_key = country + "_EX"
            df[country] = df[im_key] + df[ex_key]
            df = df.drop(columns=[im_key, ex_key])
        return df

    df = create_net_columns(df)
    df = df.rename(columns={"Date": "date"})

    df_weather = preprocessing_weatherdata.import_weatherData()
    df_weather = df_weather[df_weather['date'].dt.year >= 2015]
    # make hourly
    df_weather = make_hourly(df_weather)
    df_weather = df_weather.drop(columns=df_weather.columns[8:])
    df_weather = df_weather.drop(columns=[col for col in df_weather.columns if "percip" in col])
    df_weather = df_weather.drop(columns=['DK_sun_hrs'])

    df_24h_lag = df.copy()
    df_24h_lag.set_index("date", inplace=True)
    df_24h_lag = df_24h_lag.shift(+24)

    def custom_column_mapper(df, append_str):
        cols = df.columns
        mapper = {}
        for col in cols:
            mapper[col] = col + append_str
        return mapper

    df_24h_lag.rename(columns=custom_column_mapper(df_24h_lag, "-24h"), inplace=True)
    df_24h_lag.reset_index(inplace=True)

    df_master = df.copy()
    df_master = df_master.merge(df_24h_lag)
    df_master = df_master.merge(df_weather)

    # split up datetime information, keep date as index for easier manipulation
    df_master.set_index(['date'], append=True, inplace=True, drop=False)
    df_master = split_datetime(df_master)

    # move datetime columns to the front for readibility

    # It is bugged, i don't know why
    def move_columns_to_front(df, front_column_names):
        '''
        Returns a list with reordered column names
        '''
        columns = list(df.columns)
        for col in front_column_names:
            if col not in columns:
                front_column_names.remove(col)
        for col in columns:
            if col in front_column_names:
                columns.remove(col)
        result = front_column_names + columns
        return result

    # Static hotfix
    def move_columns_to_front_static(df, front_column_names):
        result = move_columns_to_front(df, front_column_names)
        result.pop(-1)
        result.pop(-1)
        return result

    column_order = move_columns_to_front_static(df_master,
                                                front_column_names=["year", "month", "weekday", "hour", "timestamp"])
    df_master = df_master.loc[:, column_order]
    # Save to data
    df_master.to_csv(path_to_data_dir + "df_master.csv")
    print("saved df_master to: " + path_to_data_dir+"df_master.csv")
    return df_master


def load_master_df(path_to_data_dir="./data/", force_update=False):
    '''
    Tries to load master frame from given directory. If CSV file is not present, it will create one. Set force_update = True to reload data.
    :param path_to_data_dir:
    :return:
    '''
    result = None
    try:
        if force_update:
            result = create_master_df(path_to_data_dir)
        else:
            result = pd.read_csv(path_to_data_dir + "df_master.csv", index_col=[0, 1])
    except FileNotFoundError:
        print("No master df found in specified location: creating one now. (This may take a while.)")
        result = create_master_df(path_to_data_dir)
    finally:
        return result


