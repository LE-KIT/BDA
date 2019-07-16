#!/usr/bin/env python
# coding: utf-8

from scripts.preprocessing_plannedProduction import *
from scripts.preprocessing_price_consumption import *
from scripts.preprocessing_weatherdata import *


def import_data(projectPath = '/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/'):

    df = pd.merge(left=import_weatherData(projectPath),
                  right=import_productionData(projectPath),
                on='date',
                how='outer').merge(right=import_price_consumption_Data(projectPath),
                                    on='date',
                                    how='outer')


    return df

projectPath = '/Users/ozumerzifon/Desktop/BDA-ömer_aktuell/'

df = import_data(projectPath)

df.to_csv(projectPath + 'data/master_df.csv', sep=';', decimal=',', index=False)






