# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 10:56:19 2020

@author: jorge
"""

import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pycaret.regression import *
import seaborn as sns
from windrose import WindroseAxes

#%% Functions

def uv_to_ws(u, v):
    wind_speed = np.sqrt(np.square(u) + np.square(v))
    return wind_speed

def uv_to_wd(u, v, convention='from'):
    if convention == 'to':
        wind_direction = 180/math.pi * np.arctan2(u, v)
    elif convention == 'from':
        wind_direction = 180 + 180/math.pi * np.arctan2(u, v)
    else:
        raise ValueError('Invalid convention: use "from" or "to".')
    return wind_direction

def uv_to_wswd(u, v, convention='from'):
    wind_speed = uv_to_ws(u, v)
    wind_direction = uv_to_wd(u, v, convention)
    return wind_speed, wind_direction

def plot_windrose(wd, ws):
    ax = WindroseAxes.from_ax()
    ax.bar(wd, ws, normed=True, opening=1)
    ax.set_legend()


#%% Imports and variable handling

# File namesnames
files = ['TrainData1.csv', 'WeatherForecastInput1.csv']

# Import data
train = pd.read_csv(files[0], index_col=0, parse_dates=True)
weather = pd.read_csv(files[1], index_col=0, parse_dates=True)

# Count nans and drop rows full of nan
train.isna().sum()
train.dropna(how='all', inplace=True) # only first row

# Convert wind components to speed and direction
train['ws_10'], train['wd_10'] = uv_to_wswd(train['U10'], train['V10'])
train['ws_100'], train['wd_100'] = uv_to_wswd(train['U100'], train['V100'])
weather['ws_10'], weather['wd_10'] = uv_to_wswd(weather['U10'], weather['V10'])

# Drop u and v components
train.drop(columns=['U10', 'V10', 'U100', 'V100'], inplace=True)
weather.drop(columns=['U10', 'V10', 'U100', 'V100'], inplace=True)


#%% Descriptive statistics and data visualization

plt.plot(train['POWER'])

plot_windrose(train['wd_10'], train['ws_10'])
plot_windrose(train['wd_100'], train['ws_100'])

train['POWER'].plot.hist(bins=50)
train.plot.scatter('ws_10', 'POWER', c='darkred')
train.plot.scatter('ws_100', 'POWER', c='darkblue')

#sns.pairplot(train)
sns.distplot(train['POWER'])

#%% Testing PyCaret

reg1 = setup(data = train, target = 'POWER', session_id=123)
compare_models()
