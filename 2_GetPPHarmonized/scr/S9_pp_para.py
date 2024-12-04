# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 21:19:03 2023

@author: 92978
"""

#%%
import pandas as pd
import numpy as np
import os
#import shutil
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
        #若已经存在文件追加sheet，sheet重复则替代原有sheet
        writer = pd.ExcelWriter(filename,mode='a', engine='openpyxl',if_sheet_exists='replace')
        for ish,idf in zip(sheetname,df):
            idf.to_excel(writer,sheet_name=ish,index=None)
        writer.close()

#%%
my_columns = ['Facility ID','Country', 'Facility Type','Production','Fuel Consumption','Combustion CO2 EF','Process CO2 EF']
mkdir('../output/PP_modified/')

if os.path.exists('../output/PP_parameter/Dict_PP_Para.xlsx') == 1:
    os.remove('../output/PP_parameter/Dict_PP_Para.xlsx')
        
for isec in pp_run:
    pp = pd.read_csv('../output/PP_cut/'+isec+'.csv',encoding="gbk")
    
    pp_count = pp.loc[:,my_columns]
    pp_count['Energy Efficiency'] = pp_count['Fuel Consumption']/pp_count['Production']

    region_para = pd.read_excel('../output/PP_parameter/Dict_EnergyEfficiency.xlsx',sheet_name=isec)
    region_para_set = region_para.loc[:,['Facility Type']+coun_ls].melt(id_vars=['Facility Type'],var_name='Country',value_name='Energy Efficiency Ref')
    
    change = pp_count.loc[np.isinf(pp_count['Energy Efficiency']),:]
    if change.shape[0]>0:
        change = pd.merge(change,region_para_set,on=['Facility Type','Country'])
        change['Energy Efficiency'] = change['Energy Efficiency Ref']
        change = change.loc[:,:'Energy Efficiency']
        change['Production'] = change['Fuel Consumption']/change['Energy Efficiency']
        
        pp_count.loc[np.isinf(pp_count['Energy Efficiency']),:] = change.values
    
    change = pp_count.loc[pp_count['Fuel Consumption']==0,:]
    if change.shape[0]>0:
        change = pd.merge(change,region_para_set,on=['Facility Type','Country'])
        change['Energy Efficiency'] = change['Energy Efficiency Ref']
        change = change.loc[:,:'Energy Efficiency']
        change['Fuel Consumption'] = change['Production'] * change['Energy Efficiency']
        
        pp_count.loc[pp_count['Fuel Consumption']==0,:] = change.values
    
    df_out = pp_count.loc[:,['Facility ID','Combustion CO2 EF','Process CO2 EF','Energy Efficiency']]
    pd_write_new('../output/PP_parameter/Dict_PP_Para.xlsx',[isec],[df_out])
    
    pp['Production'] = pp_count['Production']
    pp['Fuel Consumption'] = pp_count['Fuel Consumption']
    
    pp.to_csv('../output/PP_modified/'+isec+'.csv',encoding="gbk",index=None)
