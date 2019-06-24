import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta

def walk_farward_validation(df: object, training_window_days: object, evaluation_days: object,
                            processing_function: object, prediction_target: str,
                            model: object,
                            error_function: object) -> object:
    '''

    :rtype: dictionary
    :param df: the dataframe containing all input data
    :param training_window_days: number of past days that are used for the model training
    :param evaluation_days: list of days to be used as evaluation days
    :param processing_function: preprocessing function for the chosen model in the form of preprocessing_function(dataframe)
    :param prediction_taget: column name of the predicted variable
    :param model: the model that is used in the training. this needs to have a fit(X, y) method
    :param error_function: error function to be used to calculate the overall error, e.g. MAE, MSE, etc.
    :return: dictionary {"model": model name, "predicitons": dataframe, "error": overall error}
    '''

    evaluation_sets = []

    # Split the dataframe in training and evaluation data
    for day in evaluation_days:
        training_data = df[df['Date'].dt.date.between((day - timedelta(days=training_window_days)).date(), day.date())]
        evaluation_data = df[df['Date'].dt.date == day.date()]

        evaluation_sets.append({"training_data": training_data, "evaluation_data": evaluation_data})

    predictions = pd.DataFrame()

    # For each training set, fit the model and make predicitons
    for set in evaluation_sets:
        processed_training_data = processing_function(set['training_data'])
        processed_evaluation_data = processing_function(set['evaluation_data'])

        model.fit(X=processed_training_data, y=processed_training_data[prediction_target])

        prediction=pd.DataFrame({'Date': processed_evaluation_data['Date'],
                                 'Prediction': model.predict(X=processed_evaluation_data),
                                 'Actual': processed_evaluation_data['Data']})
        if(len(predictions) > 0):
            predictions.merge(prediction)
        else:
            predictions = prediction

    # Over all training sets: calculate the error given the error_function
    error = error_function(y_true=predictions['Actual'], y_pred=predictions['Prediction'])

    return({"id": model.__class__, "predictions" : predictions, "error": error})
