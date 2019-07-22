import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import pandas as pd
import numpy as np
import datetime

def predict(model,df_eval,countries):
    evalX = df_eval[['X_Attributes']].values

    evalX = np.array(evalX.tolist())
    preds = model.predict(evalX)
    
    df_predictions = pd.DataFrame(columns=countries)
    df_true_test_values = pd.DataFrame(columns=countries)
    
    ind = 0 
    test_index = df_eval[['Tag']]

    for pred in preds:
        data = pd.DataFrame(data=pred,columns=countries)
        data['Tag'] = test_index['Tag'][ind] + np.arange(24) * datetime.timedelta(hours=1)

        if ind==0:
            df_predictions = data
        else:
            df_predictions = df_predictions.append(data)

        ind+=1
        
    ind = 0 
    test_index = df_eval[['Tag']]

    for test in np.array(df_eval['NX_per_country'].tolist()):

        data = pd.DataFrame(data=test,columns=countries)

        data['Tag'] = test_index['Tag'][ind] + np.arange(24) * datetime.timedelta(hours=1)

        if ind==0:
            df_true_test_values = data
        else:
            df_true_test_values = df_true_test_values.append(data)

        ind+=1
        
    df_predictions = df_predictions.reset_index(drop=True)
    df_true_test_values = df_true_test_values.reset_index(drop=True)

    df_predictions['Tag'] =  pd.to_datetime(df_predictions['Tag'], format='%Y-%m-%d')
    df_true_test_values['Tag'] =  pd.to_datetime(df_true_test_values['Tag'], format='%Y-%m-%d %H:%M')
    
    return df_predictions , df_true_test_values

def apply_expert_knowledge(df_predictions,import_cols,export_cols,import_max_capa,export_max_capa):
    
    for col in import_cols:
        df_predictions[col] = df_predictions[col].apply(lambda row: 0 if row>0 
                                                        else 0 if  row>-1
                                                        else import_max_capa[col] if row<import_max_capa[col] else row)
    
    for col in export_cols:
        df_predictions[col] = df_predictions[col].apply(lambda row: 0 if row<0 
                                                        else 0 if row<1
                                                        else export_max_capa[col] if row>export_max_capa[col] else row)
    
    return df_predictions
    
def aggregate_to_country(df_predictions,df_true_test_values):
    
    for value in ['NL', 'CHE', 'DNK', 'CZE', 'LUX', 'SWE', 'AUT','FRA', 'PL']:
            expo = value + "_EX"
            impo = value + "_IM"
            
            df_predictions[value] = df_predictions[expo] + df_predictions[impo]
            df_predictions.drop([expo, impo], axis=1, inplace=True)
            
            df_true_test_values[value] = df_true_test_values[expo] + df_true_test_values[impo]
            df_true_test_values.drop([expo, impo], axis=1, inplace=True)
            
    return df_predictions,df_true_test_values


def plot_and_calculate_error(df_predictions,df_true_test_values,cols):
    
    for col in cols:
        plt.figure(figsize=[20,10])
        plt.plot(df_predictions['Tag'],df_predictions[col])
        plt.plot(df_true_test_values['Tag'],df_true_test_values[col])
        plt.title("Time Series Prediction Country {} for June ".format(col))
        plt.legend(['Prediction','True Data'])
        plt.format_xdata = mdates.DateFormatter('%Y-%m-%d')
        plt.show()
        
    Y=df_true_test_values[cols] 
    Y_hat=df_predictions[cols]
    
    mae = np.abs(Y - Y_hat).mean()
    print("MAE:\n{}\n\nGesamt:{}".format(mae,mae.mean()))

def calculate_prediction_errors(df_predictions,df_true_eval_values,columns):
    error_scale = np.abs(df_true_eval_values.sum())/np.abs(df_true_eval_values.sum()).sum()
    y_hat = df_predictions[columns]
    y = df_true_eval_values[columns].values

    rse_scaled = np.sqrt( ((y-y_hat)**2).sum() ) * error_scale.values
    rmse_scaled = np.sqrt( ((y-y_hat)**2).mean() ) * error_scale.values
    print('CNN LSTM Scaled RMSE: {}'.format( rmse_scaled.sum()))
    print("--------------------------\n")
