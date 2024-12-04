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
my_columns = ['Country','Facility Type','Activity rates','Production']

file_out = '../output/7_PP_parameter/Dict_EnergyEfficiency.xlsx'

mkdir('../output/7_PP_parameter/')

if os.path.exists(file_out) == 1:
    os.remove(file_out)
        
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
    
for isec in ['IronAndSteel','Power','Cement']:
    faciliy = fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']
    df_out = pd.DataFrame(columns=['Sector','Facility Type']+region_ls)
    
    for ifa in faciliy:
        df_out = pd.concat([df_out,pd.DataFrame([isec,ifa],index=['Sector','Facility Type']).T],axis=0)
        
        for ireg in region_ls:
            pp = pd.read_pickle('../output/5_RegionPP/'+isec+'_'+ifa+'_'+ireg+'.pkl')
        
            pp_count = pp.loc[(pp['Production']>0)&(pp['Activity rates']>0),my_columns]
        
            pp_count = pp_count.groupby(['Country','Facility Type'],as_index=False).sum()
            pp_count['EnergyEfficiency'] = pp_count['Activity rates']/pp_count['Production']
            
            for irow in range(pp_count.shape[0]):
                df_out.loc[(df_out['Sector']==isec)&(df_out['Facility Type']==pp_count.loc[irow,'Facility Type']),ireg] = pp_count.loc[irow,'EnergyEfficiency']
        
    pd_write_new(file_out,[isec],[df_out])