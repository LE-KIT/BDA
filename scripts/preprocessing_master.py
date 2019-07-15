#!/usr/bin/env python
# coding: utf-8

from scripts.preprocessing_plannedProduction import *
from scripts.preprocessing_price_consumption import *
from scripts.preprocessing_weatherdata import *


def import_data():

    df = pd.merge(left=import_weatherData(),
                  right=import_productionData(),
                on='date',
                how='outer').merge(right=import_price_consumption_Data(),
                                    on='date',
                                    how='outer')


    return df




