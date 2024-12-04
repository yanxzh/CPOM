# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 09:47:12 2023

@author: yanxizhe
"""

#%%
import pandas as pd
import numpy as np
from S0_GlobalENV import *
from scipy import interpolate
import sys
sys.stdout.flush()
import os

#%%
def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)            
		
def year_inter(yr_set,or_year,df):
    func=interpolate.interp1d(or_year,df[or_year],kind='linear')
    newdf = func(yr_set)
    newdf = pd.DataFrame(newdf,columns=pd.Series(yr_set).astype(int))
    
    return newdf

def df_sol(eng,dem):
    eng_inter = year_inter(yr_set=yearls,or_year=gcam_yrgap,df=eng)#插值处理，年份设计、gcam年份间隔，对应gcam数据
    dem_inter = year_inter(yr_set=yearls,or_year=gcam_yrgap,df=dem)
    
    eng_drop = eng.drop(columns=gcam_yrgap)
    dem_drop = dem.drop(columns=gcam_yrgap)
    
    eng_fin = pd.concat([eng_drop,eng_inter],axis=1)
    dem_fin = pd.concat([dem_drop,dem_inter],axis=1)
    
    return eng_fin,dem_fin

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
gcam_yrgap = [1990.0,2005.0,2010.0,2015.0,2020.0,2025.0,2030.0,2035.0,2040.0,2045.0,2050.0,2055.0,2060.0,2065,2080,2095,2100]

for isc in Eng_scenarios:
    mkdir(OUTPUT_PATH+'/'+isc+'/S1_BeforeMapping/')
    
    eng_gcam = pd.read_excel(INPUT_PATH+'/'+isc+'/pp_energy_queries.xlsx',sheet_name=None,header=1)
    demand_gcam = pd.read_excel(INPUT_PATH+'/'+isc+'/pp_demand_queries.xlsx',sheet_name=None,header=1)
    
    ele_eng,ele_dem = df_sol(eng=eng_gcam['Sheet1'],dem=demand_gcam['Sheet1'])
    cem_eng,cem_dem = df_sol(eng=eng_gcam['Sheet2'],dem=demand_gcam['Sheet2'])
    iron_eng,iron_dem = df_sol(eng=eng_gcam['Sheet3'],dem=demand_gcam['Sheet3'])
    
    eng_fn = OUTPUT_PATH+'/'+isc+'/S1_BeforeMapping/pp_energy_scenario.xlsx'
    dem_fn = OUTPUT_PATH+'/'+isc+'/S1_BeforeMapping/pp_demand_scenario.xlsx'
    pd_write_new(filename=eng_fn,sheetname=['Power','Cement','IronAndSteel'],df=[ele_eng,cem_eng,iron_eng])
    pd_write_new(filename=dem_fn,sheetname=['Power','Cement','IronAndSteel'],df=[ele_dem,cem_dem,iron_dem])