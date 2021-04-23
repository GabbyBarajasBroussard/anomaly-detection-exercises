#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import os

from env import host, user, password


# In[3]:


def get_connection(db, user=user, host=host, password=password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'


# In[25]:


def get_log_data():
    '''This function makes a connection to the MySQL server for the curriculum data. It then caches a copy to reduce run time in the future.'''
    filename = "curriculum_logs.csv"
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        # read the SQL query into a dataframe
        df = pd.read_sql('''SELECT * FROM logs 
LEFT JOIN cohorts ON logs.cohort_id = cohorts.id; ''', get_connection('curriculum_logs'))
        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename)
        # Return the dataframe to the calling code
        return df


# In[48]:


def clean_log_data():
    '''This function reads in the curriculum logs data, removes the Unnamed: 0 column, changes the date/time columns into objects, and split the date time columns. Then it drops unneeded columns.'''
    # read in data
    df= pd.read_csv('curriculum_logs.csv', index_col=0)
    #combine date and time columns
    df["datetime"] = df["date"] + ' '+ df["time"]
     #change into time columns
    df['datetime'] = pd.to_datetime(df.datetime)
    #split time columns
    df['year'] = df.datetime.dt.year
    df['month'] = df.datetime.dt.month
    df['day'] = df.datetime.dt.day
    df['hour'] = df.datetime.dt.hour
    #find day of the weeks
    df['weekday'] = df.datetime.dt.day_name()
    df = df.astype(object)
    df = df.set_index('datetime')
    
    # drop date time columns
    df.drop(columns=['id', 'slack', 'cohort_id','program_id', 'start_date', 'deleted_at', 'end_date', 'created_at', 'updated_at'], inplace = True)
    #rename cohort column
    df= df.rename(columns = {'name': 'cohort'})
    df['cohort'].astype(str)
    return df


# In[ ]:




