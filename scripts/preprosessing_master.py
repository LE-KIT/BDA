#!/usr/bin/env python
# coding: utf-8

from scripts.preprocessing_plannedProduction import *
from scripts.preprocessing_weatherdata import *
from scripts.preprocessing_trade_price_consumption import *
import pandas as pd

def import_data(useAllData = False):

    numberparse = lambda x: pd.np.float(x.replace(".", "").replace(",",".")) if x!="-" else np.nan


    df = pd.merge(left=import_weatherData(),
                  right=pd.merge(left=import_productionData(),
                                 right=import_foreignTradeData(),
                                 on='date',
                                 how='outer'),
                  on='date',
                  how='outer')

    # Use data from Trade-Price-Consumption
    if useAllData:
        df.merge(right=import_priceData(),
                 on='date',
                 how='outer').merge(right=import_consumptionData(),
                                    on='date',
                                    how='outer')


    return df


