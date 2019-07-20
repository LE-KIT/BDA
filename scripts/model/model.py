from keras.layers.convolutional import Conv2D
from keras.layers.core import Activation
from keras.layers.core import Dense,Reshape,Dropout
from keras.layers import Input,LSTM,Lambda,Flatten
from keras.models import Model
from keras.callbacks import EarlyStopping, ModelCheckpoint,LearningRateScheduler
import matplotlib.pyplot as plt
import numpy as np

def create_model(dropout,countries,add_x,rolling_days):
    num_features = len(countries+add_x)
    channels = 1
    InputShape = (channels,24*rolling_days,num_features)
    inputs = Input(shape=InputShape)
    
    x = inputs
    x = Conv2D(16,kernel_size=(2,1),strides=1,data_format="channels_first",padding='same')(x)
    x = Activation("relu")(x)
    x = Conv2D(1,1,data_format="channels_first")(x)
    x = Lambda(lambda x: x[:,0,:,:])(x)
    
    
    x = LSTM(200 , return_sequences=True)(x)

    if rolling_days>1:
        x = Lambda(lambda x: x[:,-24::,:])(x)
    
    x = Dropout(dropout)(x)
    x = Flatten()(x)
    x = Dense(24*len(countries))(x)
    x = Reshape((24, len(countries)))(x)
    
    
    return Model(inputs, x)

def plot_train_history(train_history):
    plt.figure(figsize=[10,5])
    index = np.arange(0,len(train_history.history['val_loss']))

    plt.plot(index,train_history.history['val_loss'])
    plt.plot(index,train_history.history['loss'])
    plt.legend(['Validation Loss', 'Training Loss'])
    plt.ylabel("Mean Absolute Error")
    plt.xlabel("Epoch")

    plt.show()