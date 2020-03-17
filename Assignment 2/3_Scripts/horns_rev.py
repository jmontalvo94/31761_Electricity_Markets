# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 02:17:29 2020

@author: jorge
"""
# Import libraries
import urllib.request

# Fetch data from URL
req = urllib.request.Request('http://pierrepinson.com/31761/Assignments/windpowerforecasts.dat')
with urllib.request.urlopen(req) as response:
   data = response.read()
   
#%%