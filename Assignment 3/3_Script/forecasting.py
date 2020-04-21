# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 10:56:19 2020

@author: jorge
"""

import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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
    wind_speed = np.sqrt(np.square(u) + np.square(v))
    if convention == 'to':
        wind_direction = 180/math.pi * np.arctan2(u, v)
    elif convention == 'from':
        wind_direction = 180 + 180/math.pi * np.arctan2(u, v)
    else:
        raise ValueError('Invalid convention: use "from" or "to".')
    return wind_speed, wind_direction

def plot_windrose(wd, ws, convention='to'):
    ax = WindroseAxes.from_ax()
    ax.bar(wd, ws, normed=True, opening=1)
    ax.set_legend()

#%% Imports and variable handling

# Import data
train = pd.read_csv('TrainData1.csv', index_col=0)
weather = pd.read_csv('WeatherForecastInput1.csv', index_col=0)

# Count nans
train.isna().sum()

# Drop rows full of nan (only first row)
train.dropna(how='all', inplace=True)

# Convert wind components to speed and direction
train['ws_10'], train['wd_10'] = uv_to_wswd(train['U10'], train['V10'])
train['ws_100'], train['wd_100'] = uv_to_wswd(train['U100'], train['V100'])
weather['ws_10'], weather['wd_10'] = uv_to_wswd(weather['U10'], weather['V10'])

# Drop u and v components
train.drop(columns=['U10', 'V10', 'U100', 'V100'], inplace=True)
weather.drop(columns=['U10', 'V10', 'U100', 'V100'], inplace=True)

#%% Descriptive statistics

plot_windrose(train['wd_10'], train['ws_10'])
plot_windrose(train['wd_100'], train['ws_100'])
