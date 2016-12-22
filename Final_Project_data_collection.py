# -*- coding: utf-8 -*-
"""
Created on Sun Nov 20 11:30:19 2016

@author: jhensley
"""

#Import needed packages
import pandas as pd
from datetime import  timedelta, date

'''
This first program collects a variety of electricity grid operating information
from the Midwest Independent System Operator (MISO). The range of data collected
include: 
    hourly aggregate load, 
    hourly wind production, 
    hourly generation mix,
    average locational marginal price (lmp) of electricity across MISO's main hubs,
    hourly average day-ahead eletricity price offers by technology,
    hourly total economic max capacity, economic minimum capacity, and emergency capacity
    
This data is collected and output to csv files for use in later modules

'''

#set today's date and Jan 1, 2014 and first time period to collect data
#first_date = date(year=2014,month=1,day=1) #earliest date of data available.

def get_miso_load_wind_data(periods):
    #This function imports MISO load and wind production data for the period 
    #specified. It takes in a list of periods formatted as years
    
    years = [period.year for period in periods]
    years = list(set(years))
    
    #define url and extensions
    miso_url = r'https://www.misoenergy.org/Library/Repository/Market%20Reports/'
    #create dictionary of report extensions with more familiar naming
    ext = {'hist_load': '_rfal_hist.xls', 'hist_wind': '_hwd_hist.csv'}

    #Define today
    today = date.today()
    
    #import load and wind generation data
    load = []
    wind = []
    load_cols = ['date','hour', 'region', 'forecast_load', 'actual_load']
    wind_cols = ['date', 'hour', 'wind_gen']

    for year in years:
        if year < today.year:
            date_str = str(year) + '1231'
        else:
            date_str=today.strftime('%Y%m%d')
            
        try:
            url_load = miso_url + date_str + ext['hist_load']
            url_wind = miso_url + date_str + ext['hist_wind']
        
            miso_load_data = pd.read_excel(url_load, header = 5, skip_footer =1, names = load_cols, parse_cols =[1,2,3,4,5])
            miso_wind_data = pd.read_csv(url_wind, header = 4, skip_footer =1, names = wind_cols, engine='python')
        
            load.append(miso_load_data.dropna(how='any'))
            wind.append(miso_wind_data)
        except: pass
        
    miso_load = pd.concat(load)
    miso_wind = pd.concat(wind)
    
    #reformat data
    for df in [miso_load, miso_wind]:
        #Add timestamp
        df['hour'] = df['hour'].astype(int) -1
        df['timestamp'] = pd.to_datetime(df['date'].astype(str) + df['hour'].astype(str), format = '%m/%d/%Y%H')
        
        #remove unnecessary columns and index to timestamp
        df.drop(['date', 'hour'], axis=1, inplace=True)
        #df.set_index('timestamp', inplace=True)
        
        #add balancing authority feature
        df['iso'] = 'MISO'
        
    #Return datasets
    return miso_load, miso_wind
    
def get_hist_miso_lmp(periods, csv_filename=None):
    '''This function scrapes historical miso lmp data for periods specified
    It accepts a set of periods define as number of days and a filename to 
    output a csv file with the data'''
    
    miso_url = r'https://www.misoenergy.org/Library/Repository/Market%20Reports/'
    
    lmps = []
    hubs = ['MINN.HUB', 'ILLINOIS.HUB', 'MICHIGAN.HUB', 'INDIANA.HUB', 'ARKASAS.HUB',
            'INDIANA.HUB', 'LOUISIANA.HUB', 'TEXAS.HUB']
    
    #Loop through each day and pull lmp report
    for day in periods:
        date_str = day.strftime('%Y%m%d')
        print day
        
        #Get day lmp data
        try:
            lmp_url = miso_url + date_str + '_rt_lmp_final.csv'
            miso_lmp_data = pd.read_csv(lmp_url,header = 3)
                    
            #standardize format
            lmp = pd.melt(miso_lmp_data, id_vars = ['Node', 'Type', 'Value'])
            
            #Add datetimestamp column, he= hour ending
            lmp['hour'] = lmp['variable'].apply(str.replace, args=('HE ', '')).astype(int) - 1
            lmp['timestamp'] = lmp['hour'].apply(lambda x: timedelta(hours=x)) + day
            lmp.drop(['hour','variable'], axis=1, inplace=True)
            
            #only get HUB LMPs
            lmp1 = lmp[(lmp.Node.isin(hubs)) & (lmp.Value == 'LMP')]
            #lmp1.drop(['Value', 'Type'], axis=1, inplace=True)
            lmp1.rename(columns={'value':'LMP'}, inplace=True)
            
            lmp1.set_index('timestamp', inplace=True)
            
            #add to list for concatenation later
            lmps.append(lmp1)
        
        except: pass
        
    miso_lmp = pd.concat(lmps) 
        
    #further cleansing
    miso_lmp.rename(columns = {'value':'LMP', 'Value' : 'lmp_type'}, inplace = True)
    
    #new field. ba = balancing authority
    miso_lmp['iso'] = 'MISO'
    
    #miso_lmp.to_csv(filename)
    
    #return dataframe
    return miso_lmp
    
def get_miso_mix(periods):
    #This function pulls MISO generation mix data for specified period

    #define url and extensions
    miso_url = r'https://www.misoenergy.org/Library/Repository/Market%20Reports/'
    
    gen_mix = []
    
    #Data is produced daily for years 2016 and beyond
    daily_periods = [day for day in periods if ((day.year ==2016)|((day.year==2015)&(day.month>7)))]
    
    #There is a one day lag in reporting
    daily_periods = daily_periods[0:-1]

    for day in daily_periods:
        date_str = day.strftime('%Y%m%d')
        try:
            url_mix = miso_url + date_str + '_sr_gfm.xls'
            #first three 2016 daily file formats are different
            if (day.year==2015)|((day.day < 4) & (day.month == 1)):
                mix = pd.read_excel(url_mix, skip_footer =2, header=8, parse_cols =[0,34,35,36,37,38,39])
            else:
                mix = pd.read_excel(url_mix, skip_footer =2, header=4, parse_cols =[0,26,27,28,29,30,31])
            
            #add timestamp column
            mix['hour'] = mix['Market Hour Ending'] -1
            mix['date'] = day
            mix['timestamp'] = pd.to_datetime(mix['date'].astype(str) +  mix['hour'].astype(str), format = '%Y-%m-%d%H')
            mix['timestamp'] = mix['hour'].apply(lambda x: timedelta(hours=x)) + (day)
           
            #reformatting
            mix.columns = mix.columns.str.lower()
            mix = mix[['timestamp', 'coal', 'gas', 'hydro', 'nuclear', 'wind', 'other']]
            mix['total'] = mix.hydro + mix.gas + mix.other + mix.coal + mix.wind + mix.nuclear
            
            gen_mix.append(mix)
       
        except: pass
    
    #data prior to 2016 comes in a a single file for each year and different format
    years = [min([day.year for day in periods if day.year < 2016]), max([day.year for day in periods if day.year < 2016])]
    for year in years:
        
        #MISO spreads data across different sheets
        for i in range(0,5):
            try:
                if i <1:
                    url_mix = miso_url + 'Historical_Gen_Fuel_Mix_%s.xls' %year
                    mix = pd.read_excel(url_mix, header=1, sheetname=str(year))
                    cols = mix.columns
                else:
                    mix = pd.read_excel(url_mix, names=cols, sheetname=str(year) + '(' + str(i) + ')')
            
                mix.rename(columns={'RT Generation (State Estimated), MW':'MW'}, inplace=True)
                
                mix1 = mix.groupby(['Market Date','HourEnding', 'Fuel Type']).MW.sum()
                mix2=mix1.unstack()
                
                mix3 = mix2.reset_index()
                
                #add timestamp and total columns
                mix3['hour'] = mix3['HourEnding'].astype(int) -1
                mix3['timestamp'] = pd.to_datetime(mix3['Market Date'].astype(str) +  mix3['hour'].astype(str), format = '%Y-%m-%d%H')
                mix3['total'] = mix3.COAL + mix3.GAS + mix3.HYDRO + mix3.NUCLEAR + mix3.OTHER + mix3.WIND
            
                #reformatting
                mix3 = mix3[['timestamp', 'COAL', 'GAS', 'HYDRO', 'NUCLEAR', 'WIND', 'OTHER', 'total']]
                mix3.columns = mix3.columns.str.lower()
            except: pass
        
            gen_mix.append(mix3)
    
    miso_gen_mix = pd.concat(gen_mix)
    miso_gen_mix['iso'] = 'MISO'
    
    #return dataframe
    return miso_gen_mix
    
def get_MISO_da_offers(periods):
 
    da_offers = [] #initialize empty list to hold each daily set of data
    
    #build price, MW pairs for later use
    #prices = ['Price' + str(i) for i in range(1,11)]
    #MWs = ['MW' + str(i) for i in range(1,11)]
     
    for day in periods:
        print day
        date_str = day.strftime('%Y%m%d')
        offer_url = 'https://www.misoenergy.org/Library/Repository/Market%20Reports/' + date_str + '_da_co.csv'  
        
        try:
            offers = pd.read_csv(offer_url) 
            
            #fill null values
            for i in range(1,11):
                offers['Price' + str(i)].fillna(0, inplace = True) 
                offers['MW' + str(i)].fillna(0, inplace = True) 
                   
            
            #calculate capacity weighted average offer price              
            offers['sumprod'] =(offers['Price1'] * offers['MW1'] + 
                                offers['Price2'] * offers['MW2'] +   
                                offers['Price3'] * offers['MW3'] +
                                offers['Price4'] * offers['MW4'] +
                                offers['Price5'] * offers['MW5'] +
                                offers['Price6'] * offers['MW6'] +
                                offers['Price7'] * offers['MW7'] +
                                offers['Price8'] * offers['MW8'] +
                                offers['Price9'] * offers['MW9'] +
                                offers['Price10'] * offers['MW10'])
            
            offers['weightot'] =(offers['MW1']+offers['MW2']+offers['MW3']+offers['MW4']+
                                offers['MW5']+offers['MW6']+offers['MW7']+offers['MW8']+
                                offers['MW9']+offers['MW10'])
            
            offers['wavg_price'] = offers['sumprod']/offers['weightot']
            
            #Calulate average hourly offer price by technology
            group_da_offers = offers.groupby(['Unit Type', 'Date/Time Beginning (EST)'])['wavg_price'].mean()
            group_da_offers = group_da_offers.reset_index()
            
            #Get sum of offered economic load and emergency load in each time period, by technology
            loads = offers.groupby(['Unit Type', 'Date/Time Beginning (EST)'])[['Economic Max', 'Economic Min', 'Emergency Max']].sum()
            loads.reset_index(inplace=True)
            
            #merge datasets
            offers1 = pd.merge(group_da_offers, loads)
            
            offers1['wavg_price'].fillna(0, inplace=True)
            
            #append data list
            da_offers.append(offers1)
        except: pass
    
    #create dataframe of all offer data and return it
    miso_da_offers = pd.concat(da_offers)
    miso_da_offers.rename(columns={'Date/Time Beginning (EST)':'timestamp', 'Unit Type': 'Technology'}, inplace=True)
    return miso_da_offers

def get_hist_HH_gas_price():
    #Get historical daily Henry Hub natural gas prices
    
    eia_url = 'http://www.eia.gov/dnav/ng/hist_xls/RNGWHHDd.xls'
    
    try:
        eia_hh = pd.read_excel(eia_url, skiprows=[0,1,2], names = ['Date', 'hh_price'],  sheetname='Data 1')
    except: pass

    return eia_hh

'''
def get_hist_coal_price():
    pass
'''
    
###Begin data importation process

#set list of days
year = 2014
month = 01
day = 01
days= pd.date_range(pd.datetime(year, month, day),pd.datetime.today()).tolist()
 
#import miso lmp, load, and wind generation data
print 'Getting MISO load and wind generation data'
miso_load, miso_wind = get_miso_load_wind_data(days)
 
print 'Getting MISO electricity mix data'
miso_gen_mix = get_miso_mix(days)
 
print 'Getting MISO LMP data'
miso_lmp = get_hist_miso_lmp(days)

print 'Getting MISO Day-Ahead average hourly offers by technology'
miso_da_offers = get_MISO_da_offers(days)

print 'Getting daily Henry Hub natural gas price'
eia_hh = get_hist_HH_gas_price()
    
#output the datasets to .csv files 
base_path = r'C:\Users\jhensley\Desktop\GA_Data_Science\DS-DC-16\projects\final-projects\data\\'
miso_load_path = base_path + 'miso_load.csv'
miso_wind_path = base_path + 'miso_wind.csv'
miso_gen_path = base_path + 'miso_gen_mix.csv'
miso_lmp_path = base_path + 'miso_lmp.csv'
miso_da_path = base_path + 'miso_da_offers.csv'
miso_hh_path = base_path + 'miso_eia_hh.csv'

miso_load.to_csv(miso_load_path)
miso_wind.to_csv(miso_wind_path)
miso_gen_mix.to_csv(miso_gen_path)
miso_lmp.to_csv(miso_lmp_path)
miso_da_offers.to_csv(miso_da_path)
eia_hh.to_csv(miso_hh_path)
    
