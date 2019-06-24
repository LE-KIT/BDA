from bda import evaluation
from datetime import datetime
from datetime import timedelta
import pandas as pd
from sklearn import linear_model
from sklearn.metrics import mean_absolute_error

date_range = pd.date_range(start='2019-01-01', end='2019-01-31', freq='H')
df = pd.DataFrame({"Date": date_range , 'Data': range(0,len(date_range))})
evaluation_days = [datetime(2019,1,30), datetime(2019,1,29)]
training_window_days=1


def process_df(df):
    new_df = df.copy()
    new_df['Date'] = new_df['Date'].apply(lambda x: x.timestamp())
    return new_df

result = evaluation.walk_farward_validation(df=df, training_window_days=1, evaluation_days=[datetime(2019,1,30)], processing_function=process_df, prediction_target='Data', model=linear_model.LinearRegression(), error_function=mean_absolute_error)
print(result)
