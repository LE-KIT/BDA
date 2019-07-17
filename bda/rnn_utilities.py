from datetime import timedelta
from datetime import datetime
import pandas as pd
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Dropout
from keras.layers import TimeDistributed
from keras.layers import RepeatVector
from matplotlib import pyplot
from pandas import concat
from numpy import concatenate
from sklearn.preprocessing import MinMaxScaler


def make_target_columns_first(df, country_codes=["NX","NL","CHE","DNK","CZE","LUX","SWE",'AUT', "FRA", "PL"]):
    '''
    Moves target columns (NX, NL, ...) to the front of the dataframe.
    Throws exception if passed column names are nonexisting.
    :param df:
    :param country_codes:
    :return: list with updated
    '''
    cols = [col for col in df.columns]
    cols = country_codes + cols
    cols = list(dict.fromkeys(cols))
    return cols

def get_outliers(df, factor):
    '''
    Returns the index of outliers concerning NX in a list.
    :param df:
    :param factor: factor of the std. after which entries are considered outliers.
    :return:
    '''
    idxs = df[df['NX'] > df['NX'].mean() + factor * df['NX'].std()]
    return idxs


def delete_country_columns_except(country_code, df):
    '''
    Drop every column that is not the target column
    :param country_code: country code of the target column
    :param df:
    :return: list of column names without non relevant country columns.
    '''
    columns = df.columns
    country_codes = ["NX","NL","CHE","DNK","CZE","LUX","SWE",'AUT', "FRA", "PL"]
    country_codes.remove(country_code)
    return country_codes

def find_missing_rows(df):
    '''
    Find rows where the previous entry is more than one hour in the past = find missing entries.
    :param df:
    :return: list of indices
    '''
    last_timestamp = df.iloc[0].name
    index_list = []
    index = 0
    for date, row in df.iterrows():
        current_timestamp = date
        if(current_timestamp != (last_timestamp + timedelta(hours=1))):
            index_list.append(index)
        last_timestamp = current_timestamp
        index = index + 1
    index_list.pop(0)
    return index_list

def find_days_with_missing_hours(df):
    '''
    Find days where there are one or more entries missing.
    :param df:
    :return: list of dates
    '''
    index_list = find_missing_rows(df)
    dates = []
    for idx in index_list:
        date = df.iloc[idx].name
        dates.append(date.date())
    dates = list(dict.fromkeys(dates))
    return dates


def scale_dataframe(dataframe):
    '''
    Apply sklearn MinMax(0,1) scaler to the dataframe
    :param dataframe:
    :return: (scaler object, scaled dataframe)
    '''
    # load dataset
    # dataset = dataframe.copy()
    values = dataframe.values
    # ensure all data is float
    values = values.astype('float32')
    # normalize features
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled = scaler.fit_transform(values)
    # frame as supervised learning

    return (scaler, scaled)

def make_sequence_of_length(length, array):
    '''
    Transforms a 2D array into an 3D array with ( sample_count, length, feature_dim)
    Does not work if previous length can't be divided by new length!
    :param length:
    :param array:
    :return: 3D np.array
    '''
    # check for null
    if len(array.shape) == 1:
        feature_dim = 1
    else:
        feature_dim = array.shape[1]
    return array.reshape(int(array.shape[0] / length),length, feature_dim)

def rnn_proprocessing(df, target_country, last_training_day='2019-05-31'):

    # Reorder target columns
    new_column_order = make_target_columns_first(df)
    df = df[new_column_order].copy()

    # Set index to be datetime
    df.reset_index(level=0, drop=True, inplace=True)
    df.index = pd.to_datetime(df.index)

    # Drop outliers
    df.drop(get_outliers(df, 3).index[0], inplace=True)

    # Drop non-target country columns.
    df.drop(columns=delete_country_columns_except(target_country,df), inplace=True)

    # Delete days with missing values
    # This part must not be used in a loop or it will delete more and more days,
    # bc deleted days again get counted as missing entries.
    days = find_days_with_missing_hours(df)
    for day in days:
        df.drop(df[str(day)].index, inplace=True)

    # Scale the dataframe and keep the scaler object for inverse scaling at the end.
    scaler, scaled = scale_dataframe(df)

    # Split into train and test sets depending on evaluation day start
    values = scaled
    # train until may until including last training day.
    training_entries = len(df[:last_training_day])
    # Ignore first 24 entries, because we don't know the prior day values
    train = values[24:training_entries, :]
    test = values[training_entries:, :]

    # split into input and outputs
    train_X, train_y = train[:, 1:], train[:, 0]
    test_X, test_y = test[:, 1:], test[:, 0]
    # reshape input to be 3D [samples, timesteps, features]
    train_X = make_sequence_of_length(24, train_X)
    test_X = make_sequence_of_length(24, test_X)
    # reshape output to be 3D [samples, timesteps, features]
    train_y = make_sequence_of_length(24, train_y)
    train_y = train_y.reshape(train_y.shape[0], train_y.shape[1])
    test_y = make_sequence_of_length(24, test_y)
    test_y = test_y.reshape(test_y.shape[0], test_y.shape[1])

    return (train_X, train_y, test_X, test_y)

def train_lstm(train_X, train_y, test_X, test_y, error_str="rmse", batch_size=20, epochs=50):
    '''
    Train a 2 level stacked lstm on the given data.
    :param train_X:
    :param train_y:
    :param test_X:
    :param test_y:
    :param error_str:
    :param batch_size:
    :param epochs:
    :return: model
    '''
    n_steps_in = train_X.shape[1]
    n_features = train_X.shape[2]
    n_steps_out = train_y.shape[1]

    model = Sequential()
    model.add(LSTM(200, activation='relu', return_sequences=True, input_shape=(n_steps_in, n_features)))
    model.add(LSTM(100, activation='relu'))
    model.add(Dense(n_steps_out))
    model.compile(optimizer='adam', loss=error_str)

    # fit network
    history = model.fit(train_X, train_y, epochs=100, batch_size=20, validation_data=(test_X, test_y), verbose=2,
                        shuffle=False)
    # plot history
    pyplot.plot(history.history['loss'], label='train')
    pyplot.plot(history.history['val_loss'], label='test')
    pyplot.legend()
    pyplot.show()

    return model

def get_evaluation_df(test_X, test_y, model, scaler):
    '''
    Returns a df with two columns: actual (y) and prediction (yhat)
    :param test_X:
    :param test_y:
    :param model:
    :param scaler: scaler used in preprocessing
    :return: pd.DataFrame()
    '''
    # Make prediciton  for timeframe of text_X
    yhat = model.predict(test_X)

    # Reshape data to be 2D (samples, features)
    test_X = test_X.reshape((test_X.shape[0] * test_X.shape[1], test_X.shape[2]))
    test_y = test_y.reshape((test_y.shape[0] * test_y.shape[1], 1))
    yhat = yhat.reshape((yhat.shape[0] * yhat.shape[1], 1))

    # Inverse scaling
    inv_yhat = concatenate((yhat, test_X), axis=1)
    inv_yhat = scaler.inverse_transform(inv_yhat)
    inv_y = concatenate((test_y, test_X), axis=1)
    inv_y = scaler.inverse_transform(inv_y)

    return pd.DataFrame({"y": inv_y[:,0], "yhat": inv_yhat[:,0]})

def evaluate_model(y, yhat, error_function):
    '''
    Evaluates a model given a function that expects two columns
    :param y:
    :param yhat:
    :param error_function:
    :return:
    '''
    # Make prediciton  for timeframe of text_X

    return error_function(y, yhat)

