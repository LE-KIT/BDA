import numpy as np


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
