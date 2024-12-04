# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 21:19:03 2023

@author: 92978
"""

#%%
import pandas as pd
import numpy as np
import os
import shutil
from S0_GlobalENV import *

#%%
def pd_write_new(filename,sheetname,df):
    if os.path.exists(filename) != True:
        writer = pd.ExcelWriter(filename)
        
        for ish,idf in zip(sheetname,df):
            if os.path.exists(filename) != True:
                idf.to_excel(filename,ish,index=None)
            else:
                idf.to_excel(writer,sheet_name=ish,index=None)
                
        writer.close()
                
    else:
        writer = pd.ExcelWriter(filename,mode='a', engine='openpyxl',if_sheet_exists='replace')
        for ish,idf in zip(sheetname,df):
            idf.to_excel(writer,sheet_name=ish,index=None)
        writer.close()

#%%
#计算二氧化碳
my_columns = ['Country','Facility Type','Activity rates','CO2 Emissions']

ef_com_out = '../output/7_PP_parameter/Dict_EF_CO2_Combustion.xlsx'

mkdir('../output/7_PP_parameter/')

if os.path.exists(ef_com_out) == 1:
    os.remove(ef_com_out)
        
region_ls = [
              'India',
              'China',
              'United States',
              'Russia+Eastern Europe',
              'Middle East and Africa',
              'Canada+Latin America',
              'Western Europe',
              'East Asia',
              'Other Asia and Pacific',
              ]
    
for isec in ['IronAndSteel','Power']:
    faciliy = fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']
    df_out = pd.DataFrame(columns=['Sector','Facility Type']+region_ls)
    
    for ifa in faciliy:
        df_out = pd.concat([df_out,pd.DataFrame([isec,ifa],index=['Sector','Facility Type']).T],axis=0)
        
        for ireg in region_ls:
            pp = pd.read_pickle('../output/5_RegionPP/'+isec+'_'+ifa+'_'+ireg+'.pkl')
        
            pp_count = pp.loc[pp['Production']>0,my_columns]
        
            pp_count = pp_count.groupby(['Country','Facility Type'],as_index=False).sum()
            pp_count['Combustion CO2 EF'] = pp_count['CO2 Emissions']/pp_count['Activity rates']
            
            for irow in range(pp_count.shape[0]):
                df_out.loc[(df_out['Sector']==isec)&(df_out['Facility Type']==pp_count.loc[irow,'Facility Type']),ireg] = pp_count.loc[irow,'Combustion CO2 EF']
        
    pd_write_new(ef_com_out,[isec],[df_out])
    
#%%
my_columns = ['Country','Facility Type','Activity rates','CO2 Emissions']

ef_com_out = '../output/7_PP_parameter/Dict_EF_CO2_Combustion_ce.xlsx'
ef_pro_out = '../output/7_PP_parameter/Dict_EF_CO2_Process_ce.xlsx'

mkdir('../output/7_PP_parameter/')

if os.path.exists(ef_com_out) == 1:
    os.remove(ef_com_out)
if os.path.exists(ef_pro_out) == 1:
    os.remove(ef_pro_out)
    
region_ls = [
              'India',
              'China',
              'United States',
              'Russia+Eastern Europe',
              'Middle East and Africa',
              'Canada+Latin America',
              'Western Europe',
              'East Asia',
              'Other Asia and Pacific',
              ]

isec = 'Cement'

gid_data = pd.read_pickle('../input/PP/GID_database_'+isec+'.pkl')
gid_data = gid_data.loc[(gid_data['ID2 Name']!='Cement production'),:].reset_index(drop=True)

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

gid_data_pro = gid_data.loc[(gid_data['Activity type']=='Clinker Production'),:].reset_index(drop=True)
gid_data_comb = gid_data.loc[gid_data['Activity type']=='Energy Consumption',:].reset_index(drop=True)

#涉及到的设备
faciliy = fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']
df_out = pd.DataFrame(columns=['Sector','Facility Type']+region_ls)

for ifa in faciliy:
    df_out = pd.concat([df_out,pd.DataFrame([isec,ifa],index=['Sector','Facility Type']).T],axis=0)
    
    for ireg in region_ls:
        pp = pd.read_pickle('../output/5_RegionPP/'+isec+'_'+ifa+'_'+ireg+'.pkl')
        ori_pro = gid_data_pro.loc[np.isin(gid_data_pro['Plant ID'],pp['Plant ID']),['Activity rates','CO2 Emissions']]
        df_out.loc[(df_out['Sector']==isec),ireg] = ori_pro['CO2 Emissions'].sum()/ori_pro['Activity rates'].sum()
    
pd_write_new(ef_pro_out,[isec],[df_out])

df_out = pd.DataFrame(columns=['Sector','Facility Type']+region_ls)

for ifa in faciliy:
    df_out = pd.concat([df_out,pd.DataFrame([isec,ifa],index=['Sector','Facility Type']).T],axis=0)
    
    for ireg in region_ls:
        pp = pd.read_pickle('../output/5_RegionPP/'+isec+'_'+ifa+'_'+ireg+'.pkl')
        ori_pro = gid_data_comb.loc[np.isin(gid_data_comb['Plant ID'],pp['Plant ID']),['Activity rates','CO2 Emissions']]
        df_out.loc[(df_out['Sector']==isec),ireg] = ori_pro['CO2 Emissions'].sum()/ori_pro['Activity rates'].sum()
    
pd_write_new(ef_com_out,[isec],[df_out])