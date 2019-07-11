import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta

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
    "Merges" split up datetime back together. Returning Dataframe only has one date column.
    :param df: pd.DataFrame with a column named "timestamp" containing a np.datetime64 timestamp.
    :return: pd.DataFrame 
    '''
    df['date']=df['timestamp'].apply(lambda x: (x * np.timedelta64(1,'s') + np.datetime64('1970-01-01T00:00:00Z')))
    df = df.drop(columns = ["year","month","weekday", "timestamp", "hour"])
    return df