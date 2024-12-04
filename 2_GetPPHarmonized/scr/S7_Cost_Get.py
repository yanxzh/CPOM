# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 14:04:10 2024

@author: 92978
"""

#%%
import numpy as np
import pandas as pd
from S0_GlobalENV import *

#%%
def get_fac_data(df,sec,mk):
    col = ['Plant ID', 'Country', 'Longitude', 'Latitude', 'Plant Name', 'Sector',
           'Facility ID', 'Facility Type', 'Fuel Type', 'Start Year', 'Close Year',
           'Capacity', 'Capacity Unit', 'Year', 'Activity type','Activity rates',
           'Activity rates Unit', 'CO2 Eta (%)',]
    gid_info = df.loc[:,col].drop_duplicates(['Facility ID'])

    gid_emis = df.loc[:,['Facility ID','CO2 Emissions']].groupby(['Facility ID'],as_index=False).sum()
    final_facility_data = pd.merge(gid_info,gid_emis,on='Facility ID',how='left')
    
    if mk=='prod':
        final_facility_data.rename(columns={'CO2 Emissions':'CO2 Emissions_prod',
                                            'Activity rates':'Production',
                                            'Activity rates Unit':'Production Unit'},inplace=True)
    elif mk=='ener':
        final_facility_data.rename(columns={'CO2 Emissions':'CO2 Emissions_ener'},inplace=True)
    
    return final_facility_data

def get_iron(isec):
    
    print(isec,flush=True)
    gid_data = pd.read_pickle('../input/PP/GID_database_'+isec+'.pkl')
    use_col = ['Plant ID','Country','Longitude','Latitude',
               'Plant Name', 'Sector', 'Facility ID', 'Facility Type',
               'Fuel Type', 'Start Year', 'Close Year', 'Capacity', 'Capacity Unit',
               'Year', 'Activity rates', 'Activity type', 'Activity rates Unit',
               'CO2 Eta (%)', 'CO2 Emissions']
    
    gid_data.loc[gid_data['Capacity Unit']=='kt','Capacity'] = gid_data.loc[gid_data['Capacity Unit']=='kt','Capacity']/10**3
    gid_data.loc[gid_data['Capacity Unit']=='kt','Capacity Unit'] = 'Mt'
    
    filtered = (pd.isnull(gid_data['Longitude'])==0)&(gid_data['Year']==2020)&(gid_data['Longitude']!=' ')
    gid_data = gid_data.loc[filtered,use_col].reset_index(drop=True)
    
    for irow in ['Longitude','Latitude','CO2 Emissions','Capacity','Activity rates']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(float)
    del irow
    for irow in ['Start Year','Close Year','Year']:
        gid_data.loc[:,irow] =  gid_data.loc[:,irow].replace(' ',9999)
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(int)
    del irow
    
    gid_data = gid_data.loc[~(gid_data['Longitude']<-180),:].reset_index(drop=True)
    
    gid_data_pro = gid_data.loc[gid_data['Activity type']=='Production',:].reset_index(drop=True)
    gid_data_comb = gid_data.loc[gid_data['Activity type']=='Energy Consumption',:].reset_index(drop=True)
    
    gid_data_pro = get_fac_data(df=gid_data_pro,sec=isec,mk='prod')
    gid_data_comb = get_fac_data(df=gid_data_comb,sec=isec,mk='ener')
    
    gid_data = pd.merge(gid_data_comb,
                        gid_data_pro.loc[:,['Facility ID','Production','Production Unit','CO2 Emissions_prod']],
                        on='Facility ID',how='left')
    
    gid_data['Production Unit'] = 'kt'
    gid_data.loc[:,['Production','CO2 Emissions_prod']] = gid_data.loc[:,['Production','CO2 Emissions_prod']].fillna(0)
    gid_data['CO2 Emissions'] = gid_data['CO2 Emissions_prod']+gid_data['CO2 Emissions_ener']
    gid_data.drop(['CO2 Emissions_prod','CO2 Emissions_ener'],axis=1,inplace=True)
    
    gid_data = gid_data.loc[:,['Plant ID','Facility ID','Facility Type','Capacity','Capacity Unit']]
    
    return gid_data

def get_cement(isec):
    gid_data = pd.read_pickle('../input/PP/GID_database_'+isec+'.pkl')
    use_col = ['Plant ID','Country','Longitude','Latitude',
               'Plant Name', 'Sector', 'Facility ID', 'Facility Type',
               'Fuel Type', 'Start Year', 'Close Year', 'Capacity', 'Capacity Unit',
               'Year', 'Activity rates', 'Activity type', 'Activity rates Unit',
               'CO2 Eta (%)', 'CO2 Emissions']
    
    filtered = (gid_data['CO2 Emissions']>0)&(pd.isnull(gid_data['Longitude'])==0)&(gid_data['Year']==2020)
    gid_data = gid_data.loc[filtered,use_col].reset_index(drop=True)

    for irow in ['Longitude','Latitude','CO2 Emissions','Capacity','Activity rates']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(float)
    del irow
    for irow in ['Start Year','Close Year','Year']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(int)
    del irow
    
    gid_data_pro = gid_data.loc[gid_data['Activity type']=='Clinker Production',:].reset_index(drop=True)
    gid_data_comb = gid_data.loc[gid_data['Activity type']=='Energy Consumption',:].reset_index(drop=True)
    
    gid_data_pro = get_fac_data(df=gid_data_pro,sec=isec,mk='prod')
    gid_data_comb = get_fac_data(df=gid_data_comb,sec=isec,mk='ener')
    
    gid_data = pd.merge(gid_data_pro,
                        gid_data_comb.loc[:,['Facility ID','Activity rates','Activity rates Unit','CO2 Emissions_ener']],
                        on='Facility ID',how='outer')
    
    gid_data['Production Unit'] = 'kt'
    gid_data.loc[:,['Activity rates','CO2 Emissions_ener','Production','CO2 Emissions_prod']] =\
        gid_data.loc[:,['Activity rates','CO2 Emissions_ener','Production','CO2 Emissions_prod']].fillna(0)
    gid_data['CO2 Emissions'] = gid_data['CO2 Emissions_prod']+gid_data['CO2 Emissions_ener']
    gid_data.drop(['CO2 Emissions_prod','CO2 Emissions_ener'],axis=1,inplace=True)
    
    gid_data = gid_data.loc[:,['Plant ID','Facility ID','Facility Type','Capacity','Capacity Unit']]
    
    return gid_data

def get_power(isec):
    
    print(isec,flush=True)
    gid_data = pd.read_pickle('../input/PP/GID_database_'+isec+'.pkl')
    use_col = ['Plant ID','Country','Longitude','Latitude',
               'Plant Name', 'Sector', 'Facility ID', 'Facility Type',
               'Fuel Type', 'Start Year', 'Close Year', 'Capacity', 'Capacity Unit',
               'Year', 'Activity rates', 'Activity type', 'Activity rates Unit',
               'CO2 Eta (%)','CO2 Emissions','Generation','Generation_Unit']
    
    filtered = (gid_data['CO2 Emissions']>0)&(pd.isnull(gid_data['Longitude'])==0)&(gid_data['Year']==2020)
    gid_data = gid_data.loc[filtered,use_col].reset_index(drop=True)

    for irow in ['Longitude','Latitude','CO2 Emissions','Capacity','Activity rates']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(float)
    del irow
    for irow in ['Start Year','Close Year','Year']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(int)
    del irow
    
    gid_data.rename(columns={'Generation':'Production','Generation_Unit':'Production Unit'},
                    inplace=True)

    gid_data['Production'] = gid_data['Production']*10**6
    gid_data['Production Unit'] = 'kWh'
    
    gid_data = gid_data.loc[:,['Plant ID','Facility ID','Facility Type','Capacity','Capacity Unit']]
    
    return gid_data

    #%%
if __name__ == '__main__':
    gid_is = get_iron(isec='IronAndSteel')
    gid_ce = get_cement(isec='Cement')
    gid_po = get_power(isec='Power')
    
    gid_all = pd.concat([gid_is,gid_ce,gid_po],axis=0)
    gid_all.reset_index(drop=True,inplace=True)

    cost_dict = pd.read_excel('../input/Dict_cost/Dict_RetrofitCost.xlsx',sheet_name='Final')
    gid_all['CaptureCost'] = gid_all['Facility Type'].replace(cost_dict['Facility Type'].values,
                                                              cost_dict['UnitCaptureCost'].values)
    
    gid_all['CaptureCost'] = gid_all['CaptureCost']*gid_all['Capacity']

    plant_cost = gid_all.loc[:,['Plant ID','CaptureCost']]
    plant_cost = plant_cost.groupby(['Plant ID'],as_index=False).sum()
    
    gid_all.to_csv('../output/UnitCaptureCost.csv',index=None)
    plant_cost.to_csv('../output/PlantCaptureCost.csv',index=None)