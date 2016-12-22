# -*- coding: utf-8 -*-
"""
Created on Wed Dec 07 10:26:30 2016

@author: jhensley
"""
 
'''
This module of the project parses, cleans, and merges the data collected.
The end result is a polished dataset that can be visually explored and analyzed
'''   

import pandas as pd
import statsmodels.api as sm


#datasets to import
base_path = r'C:\Users\jhensley\Desktop\GA_Data_Science\DS-DC-16\projects\final-projects\data\\'
miso_load_path = base_path + 'miso_load.csv'
miso_wind_path = base_path + 'miso_wind.csv'
miso_gen_path = base_path + 'miso_gen_mix.csv'
miso_lmp_path = base_path + 'miso_lmp.csv'
miso_da_path = base_path + 'miso_da_offers.csv'
miso_hh_path = base_path + 'miso_eia_hh.csv'

#import each file
miso_load = pd.DataFrame.from_csv(miso_load_path)
miso_wind = pd.DataFrame.from_csv(miso_wind_path)
miso_mix = pd.DataFrame.from_csv(miso_gen_path)
miso_lmp = pd.DataFrame.from_csv(miso_lmp_path)
miso_offers = pd.DataFrame.from_csv(miso_da_path)
eia_hh = pd.DataFrame.from_csv(miso_hh_path)

#Reshape Miso load data
miso_load['timestamp']=pd.to_datetime(miso_load['timestamp'])
miso_load = miso_load[miso_load.region == 'MISO'] #Get total ISO load
miso_load.reset_index(inplace=True)
miso_load.sort_values(by='timestamp', inplace=True) #Sort by date and time
miso_load = miso_load[['timestamp','iso','forecast_load', 'actual_load']]
#Create new feature quantifying the forecasting error
miso_load['forecast_error'] = miso_load.forecast_load - miso_load.actual_load
#Set timestamp as the index
miso_load.set_index('timestamp', inplace=True)

#Reshape MISO Wind generation data
miso_wind['timestamp']=pd.to_datetime(miso_wind['timestamp'])
miso_wind.sort_values(by='timestamp', inplace=True)
miso_wind.set_index('timestamp', inplace=True)

#Reshape MISO Generation Mix data
miso_mix['timestamp']=pd.to_datetime(miso_mix['timestamp'])
miso_mix.sort_values(by='timestamp', inplace=True)
miso_mix.set_index('timestamp', inplace=True)

#Reshapse MISO LMP data
miso_lmp.index=pd.to_datetime(miso_lmp.index)
miso_lmp1 = miso_lmp.groupby([miso_lmp.index, miso_lmp.iso]).LMP.mean()
miso_lmp1 = miso_lmp1.reset_index()
miso_lmp1.set_index('timestamp', inplace=True)
miso_lmp1.sort_index(inplace=True)

#Reshape Day-Ahead Offers data

#First, need to map the technology name to value. Rename Tech col values
tech_map =  {1:'Single Boiler',
             2:'Multiple Boiler',
             3:'Bleed Steam Unit',
             4:'Steam Turbine',
             5:'Combine Cycle ST',
             11:'Boiling Water Reactor',
             12:'Pressurized Water Reactor',
             21:'Industrial CT',
             22:'Single Engine Jet',
             23:'Two Engine Jet - One Expander Turbines',
             24:'Two Engine Jet - Two Expander Turbines',
             25:'Eight Engine Jet',
             26:'Regenerative Unit',
             27:'Combustion Turbine',
             31:'All Diesel Units',
             41:'Run of River',
             42:'Pumped Storage',
             51:'Combined Cycle CT',
             52:'Combined Cycle Aggregate',
             61:'Wind',
             71:'Other Fossil',
             72:'Other Peaker',
             86:'Steam Max Schedule',
             87:'DR Type1',
             88:'DR Type2',
             89:'External Resource',
             99:'Demand Response'}
miso_offers['Technology'].replace(tech_map, inplace=True)

#Convert timestamp column to a pandas DateTime data type
miso_offers['timestamp'] = pd.to_datetime(miso_offers.timestamp)

#Pivot table so that we have a column for each combination of technology
#and (wavg_price, Economic Max, Economic Min, Emergency Max)
#This achieves a single row for all hourly periods
miso_offers1 =miso_offers.pivot_table(values=['wavg_price', 'Economic Max', 'Economic Min', 'Emergency Max'],
                        index='timestamp', columns='Technology')

miso_offers1.columns = [str(b) + ' ' + str(a) for a,b in miso_offers1.columns]

miso_offers1.sort_index(inplace=True)

#Reshape EIA Henry Hub Electricity Price data
eia_hh['timestamp']=pd.to_datetime(eia_hh['Date'])
eia_hh.set_index('timestamp', inplace=True)
eia_hh1 = eia_hh.resample('h')
eia_hh1=eia_hh1.ffill()
eia_hh1.drop('Date', axis=1, inplace=True)


'''
Now that the datasets are all in a similar format and shape, it is time to
merge them together into one dataset. Merging on the timestamp index
'''

#create list of all datsets
sets = [miso_load, miso_wind, miso_lmp1, miso_mix, miso_offers1, eia_hh1]

#Limited to time period where datasets all line up. Earliest data available
#for most datasets is Jan 1, 2014. The day-ahead offers dataset is the limiting
#dataset in terms of most recent data

first_date = miso_load.index[0]
last_date = miso_offers1.index[-1]

miso_load = miso_load[first_date:last_date]
miso_wind = miso_wind[first_date:last_date]
miso_lmp1 = miso_lmp1[first_date:last_date]
miso_mix = miso_mix[first_date:last_date]
miso_offers1 = miso_offers1[first_date:last_date]
eia_hh1 = eia_hh1[first_date:last_date]

#Merge datasets
miso = reduce(lambda left, right: pd.merge(left,right,how='inner',left_index=True, right_index=True),sets)
miso.drop(['iso_x', 'iso_y'], axis=1, inplace=True)

'''
FEATURE ENGINEERING
'''

#Identify peak hours, 6:00 to 22:00 EST
miso['Peak'] = miso.index.hour
miso.loc[(miso.Peak<7)|(miso.Peak>21), 'Peak']  = 0 
miso.loc[miso.Peak.between(6,22), 'Peak']  = 1
         
#Calculate wind market penetration
miso['wind_share'] = miso['wind_mwh']/miso['actual_load']
        
miso


'''
Output polished dataset'
'''
miso.to_csv(base_path + 'miso.csv')