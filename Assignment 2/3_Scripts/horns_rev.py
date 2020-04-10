# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 02:17:29 2020

@author: jorge
"""
# Import libraries
import pandas as pd

# Fetch data from URL
rawdata_url = 'http://pierrepinson.com/31761/Assignments/windpowerforecasts.dat'
df = pd.read_csv(rawdata_url, sep=';')

# Count NaN
df.isna().sum()

#%%