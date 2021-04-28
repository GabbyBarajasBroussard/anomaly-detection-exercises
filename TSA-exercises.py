#!/usr/bin/env python
# coding: utf-8

# In[1]:


from __future__ import division
import itertools
import warnings
warnings.filterwarnings("ignore")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from numpy import linspace, loadtxt, ones, convolve
from sklearn.ensemble import IsolationForest
import numpy as np
import pandas as pd
import collections
import math
from sklearn import metrics
from random import randint
from matplotlib import style
import seaborn as sns
# style.use('fivethirtyeight')
get_ipython().run_line_magic('matplotlib', 'inline')

pd.plotting.register_matplotlib_converters()


# In[4]:


def make_log_data():
    '''This function reads in the names of the columns and the csv holding the anonymized curriculum data to make a dataframe.'''
    colnames = ['date', 'endpoint', 'user_id', 'cohort_id', 'source_ip']
    df = pd.read_csv("anonymized-curriculum-access.txt", 
                 sep="\s", 
                 header=None, 
                 names = colnames, 
                 usecols=[0, 2, 3, 4, 5])
    return df
df= make_log_data()
df.head()


# In[6]:


def prep_log_data(user, span, weight):
    ''' This function uses the dataframe created previously, allows the user to be specified, converts the date column to a date/time column, makes the index column the date,
    and returns a pd.series called pages which shows the total pages accessed by the user.''' 
    df=make_log_data()
    df = df[df.user_id == user]
    df.date = pd.to_datetime(df.date)
    df = df.set_index(df.date)
    pages = df['endpoint'].resample('d').count()
    return df, pages


# In[ ]:


def compute_bollinger(pages, span, weight, user):
    
    ''' This function calculates the lower, mid, and upper bands and the standard deviation. 
    The function then concats the bands and the pages to the dataframe. Finally a new dataframe with
    the pages and bands is returned.
 '''
    
    midband = pages.ewm(span=span).mean()
    stdev = pages.ewm(span=span).std()
    ub = midband + stdev*weight
    lb = midband - stdev*weight
    bb = pd.concat([ub, lb], axis=1)
    bol_df = pd.concat([pages, midband, bb], axis=1)
    bol_df.columns = ['pages', 'midband', 'ub', 'lb']
    bol_df['pct_b'] = (bol_df['pages'] - bol_df['lb'])/(bol_df['ub'] - bol_df['lb'])
    bol_df['user_id'] = user
    
    return bol_df


# In[ ]:


def plt_bands(b_df, user):
    
    ''' This functions will plot the upper, mid, and lower bands and original count of page accessess for every user.'''
    
    fig, ax = plt.subplots(figsize=(12,8))
    ax.plot(b_df.index, b_df.pages, label='Number of Pages, User: '+str(user))
    ax.plot(b_df.index, b_df.midband, label = 'Middle band')
    ax.plot(b_df.index, b_df.ub, label = 'Upper Band')
    ax.plot(b_df.index, b_df.lb, label = 'Lower Band')
    ax.set_ylabel('Number of Pages Accessed')
    
    return plt.show()
    

