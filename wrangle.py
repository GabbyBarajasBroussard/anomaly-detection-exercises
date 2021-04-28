#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import os

from env import host, user, password


########################################################################################################################################################################### 

def get_connection(db, user=user, host=host, password=password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'


########################################################################################################################################################################### 


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

###########################################################################################################################################################################    


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
    #rename cohort column
    df= df.rename(columns = {'name': 'cohort'})
    df['cohort'].astype(str)
    df['is_data_science'] = (df.cohort.str.contains('Ada') | df.cohort.str.contains('Bayes') | df.cohort.str.contains('Curie') | df.cohort.str.contains('Darden') | df.cohort.str.contains('Easley')| df.cohort.str.contains('florence')) 
    # drop date time columns
    df.drop(columns=['slack','program_id', 'start_date', 'deleted_at', 'end_date', 'created_at', 'updated_at'], inplace = True)
    
    return df

########################################################################################################################################################################### 
def get_zillow_data():
    '''This function will connect to the Codeup Student Database. It will then cache a local copy to the computer to use for later
        in the form of a CSV file. If you want to reproduce the results, you will need your own env.py file and database credentials.'''
    filename = "zillow_db.csv"
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        # read the SQL query into a dataframe
        df = pd.read_sql('''
SELECT prop.*, 
       pred.logerror, 
       pred.transactiondate, 
       air.airconditioningdesc, 
       arch.architecturalstyledesc, 
       build.buildingclassdesc, 
       heat.heatingorsystemdesc, 
       landuse.propertylandusedesc, 
       story.storydesc, 
       construct.typeconstructiondesc 
FROM   properties_2017 prop  
       INNER JOIN (SELECT parcelid,
                          logerror,
                          Max(transactiondate) transactiondate 
                   FROM   predictions_2017 
                   GROUP  BY parcelid, logerror) pred
               USING (parcelid) 
       LEFT JOIN airconditioningtype air USING (airconditioningtypeid) 
       LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid) 
       LEFT JOIN buildingclasstype build USING (buildingclasstypeid) 
       LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid) 
       LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid) 
       LEFT JOIN storytype story USING (storytypeid) 
       LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid) 
WHERE  prop.latitude IS NOT NULL 
       AND prop.longitude IS NOT NULL
       AND (propertylandusetypeid IN (261, 262, 263, 264, 268, 273, 274, 276, 279));
            ''' , get_connection('zillow'))
        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv('zillow_db.csv')
        # Return the dataframe to the calling code
        return df
###########################################################################################################################################################################    
def wrangle_zillow():
    
    '''
    This function acquires the Zillow data from Codeup's database on the MySQL server.  
    
    It then prepares the data by removing columns and rows that are missing more than 50% of the 
    data, restricts the dataframe to include only single unit properties, with at least one
    bedroom and bathroom and at least 500 square feet, adds a column to indicate county (based on 
    fips), drops any unnecessary columns, adjusts for outliers in taxvaluedollarcnt and
    calculatedfinishedsquarefeet, fills missing values in buildinglotsize and buildingquality with 
    median values, and renames columns to user-friendly titles.
    '''
    df = pd.read_csv('zillow_db.csv', index_col=0)
    
    #change fips to int
    df.fips = df.fips.astype(int)
    
    # Restrict df to only properties that meet single unit use criteria
    single_use = [261, 262, 263, 264, 266, 268, 273, 276, 279]
    df = df[df.propertylandusetypeid.isin(single_use)]
    
    # Restrict df to only those properties with at least 1 bath & bed and 500 sqft area
    df = df[(df.bedroomcnt > 0) & (df.bathroomcnt > 0) & ((df.unitcnt<=1)|df.unitcnt.isnull())\
            & (df.calculatedfinishedsquarefeet>=500)]

    # Handle missing values i.e. drop columns and rows based on a threshold
    df = handle_missing_values(df)
    
    # Add column for counties
    df['county'] = np.where(df.fips == 6037, 'Los_Angeles',
                           np.where(df.fips == 6059, 'Orange', 
                                   'Ventura'))    
    # drop columns not needed
    df = df.drop(columns = ['id','calculatedbathnbr', 'finishedsquarefeet12', 'fullbathcnt', 'heatingorsystemtypeid'
       ,'propertycountylandusecode', 'propertylandusetypeid','propertyzoningdesc', 
        'censustractandblock', 'rawcensustractandblock',  'propertylandusedesc'])

    # replace nulls in unitcnt with 1
    df.unitcnt.fillna(1, inplace = True)
    
    # assume that since this is Southern CA, null means 'None' for heating system
    df.heatingorsystemdesc.fillna('None', inplace = True)
    
    # replace nulls with median values for select columns
    df.lotsizesquarefeet.fillna(7313, inplace = True)
    df.buildingqualitytypeid.fillna(6.0, inplace = True)

    # Columns to look for outliers
    df = df[df.taxvaluedollarcnt < 5_000_000]
    df[df.calculatedfinishedsquarefeet < 8000]
    
    # Just to be sure we caught all nulls, drop them here
    df = df.dropna()
   
    #recalculate yearbuilt to age of home:
    df.yearbuilt = 2017 - df.yearbuilt 
    #rename columns:
    df.rename(columns={'taxvaluedollarcounty':'tax_value', 'bedroomcnt':'bedrooms', 'bathroomcnt':'bathrooms', 'calculatedfinishedsquarefeet':
                      'square_feet', 'lotsizesquarefeet':'lot_size', 'buildingqualitytypeid':'buildingquality', 'yearbuilt':'age', 'taxvaluedollarcnt': 'tax_value', 'landtaxvaluedollarcnt': 'land_tax_value', 'unitcnt': 'unit_count', 'heatingorsystemdesc': 'heating_system', 'structuretaxvaluedollarcnt': 'structure_tax_value'}, inplace=True)

    # create taxrate variable
    df['tax_rate'] = df.taxamount/df.tax_value*100

    # create acres variable
    df['acres'] = df.lot_size/43560
    
    # dollar per square foot-structure
    df['structure_dollar_per_sqft'] = df.structure_tax_value/df.square_feet

    # dollar per square foot-land
    df['land_dollar_per_sqft'] = df.land_tax_value/df.lot_size

    # ratio of bathrooms to bedrooms
    df['bath_bed_ratio'] = df.bathrooms/df.bedrooms

    # 12447 is the ID for city of LA. 
    # I confirmed through sampling and plotting, as well as looking up a few addresses.
    df['cola'] = df['regionidcity'].apply(lambda x: 1 if x == 12447.0 else 0)
    
    df = df.drop(columns=['parcelid', 'buildingquality', 'county', 'lot_size', 'regionidcity',
       'regionidcounty', 'regionidzip', 'roomcnt', 'unit_count', 'assessmentyear', 'transactiondate', 'heating_system'])
  
    
    return df
###########################################################################################################################################################################    
def get_grocery_data():
    '''This function will connect to the Codeup Student Database. It will then cache a local copy to the computer to use for later
        in the form of a CSV file. If you want to reproduce the results, you will need your own env.py file and database credentials.'''
    filename = "grocery_db.csv"
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        # read the SQL query into a dataframe
        df = pd.read_sql('''
select *
from grocery_customers
''' , get_connection('grocery_db'), index_col="customer_id")
        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv('grocery_db.csv')
        # Return the dataframe to the calling code
        return df
    
###########################################################################################################################################################################

def handle_missing_values(df, prop_required_column = .5, prop_required_row = .70):
    '''
    This function takes in: a dataframe, the proportion (0-1) of rows (for each column) with non-missing values required to keep 
    the column, and the proportion (0-1) of columns/variables with non-missing values required to keep each row.  
    
    It returns the dataframe with the columns and rows dropped as indicated. 
    '''
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df

###########################################################################################################################################################################

def nulls_by_col(df):
    '''
    This function takes in a dataframe of observations and attributes and returns a dataframe where each row is an attribute name, 
    the first column is the number of rows with missing values for that attribute, and the second column is percent of total rows 
    that have missing values for that attribute. 
    '''
    
    num_missing = df.isnull().sum()
    rows = df.shape[0]
    pct_missing = num_missing / rows
    cols_missing = pd.DataFrame({'number_missing_rows': num_missing, 'percent_rows_missing': pct_missing})
    return cols_missing

###########################################################################################################################################################################

def cols_missing(df):
    '''
    This function takes in a dataframe and returns a dataframe with 3 columns: the number of columns missing, 
    percent of columns missing, and number of rows with n columns missing. 
    '''
    
    df2 = pd.DataFrame(df.isnull().sum(axis =1), columns = ['num_cols_missing']).reset_index()\
    .groupby('num_cols_missing').count().reset_index().\
    rename(columns = {'index': 'num_rows' })
    df2['pct_cols_missing'] = df2.num_cols_missing/df.shape[1]
    return df2