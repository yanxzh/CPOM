# -*- coding: utf-8 -*-
"""
Created on Sun Feb 19 15:57:02 2023

@author: 92978
"""

#%%
import pandas as pd
import numpy as np
from S0_GlobalENV import *
import os
import shutil
import sys
sys.stdout.flush()

#%%
def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)     
        
def pd_write_new(filename,sheetname,df):
    if os.path.exists(filename) != True:
        writer = pd.ExcelWriter(filename,engine='openpyxl')
        
        for ish,idf in zip(sheetname,df):
            if os.path.exists(filename) != True:
                idf.to_excel(filename,ish,index=None)
            else:
                idf.to_excel(writer,sheet_name=ish,index=None)
        
        # writer.save()
        writer.close()
                
    else:
        writer = pd.ExcelWriter(filename,mode='a', engine='openpyxl',if_sheet_exists='replace')
        for ish,idf in zip(sheetname,df):
            idf.to_excel(writer,sheet_name=ish,index=None)
                 
        # writer.save()
        writer.close()
            
def power_work(eng,dem):
    fuelmap = pd.read_excel('../input/dict/FuelMapping.xlsx',sheet_name='Power')
    fuel_out = ['Coal', 'Gas', 'Oil','Biomass']
    
    dem_fuel = dem.groupby(['subsector','region'],as_index=False).sum()
    dem_fuel.replace(fuelmap['fuel_gcam'].values,fuelmap['fuel_gid'].values,inplace=True)
    dem_fuel = dem_fuel.loc[np.isin(dem_fuel['subsector'],fuel_out)]
    
    ccs_dem_fuel = dem.loc[dem['technology'].str.contains('CCS'),:].groupby(['subsector','region'],as_index=False).sum()
    ccs_dem_fuel.replace(fuelmap['fuel_gcam'].values,fuelmap['fuel_gid'].values,inplace=True)
    ccs_dem_fuel = ccs_dem_fuel.loc[np.isin(ccs_dem_fuel['subsector'],fuel_out)]
    
    eng_fuel = eng.groupby(['subsector','region'],as_index=False).sum()
    eng_fuel.replace(fuelmap['fuel_gcam'].values,fuelmap['fuel_gid'].values,inplace=True)
    eng_fuel = eng_fuel.loc[np.isin(eng_fuel['subsector'],fuel_out)]
    
    for ifuel in fuel_out:
        dem_trend = dem_fuel.loc[dem_fuel['subsector']==ifuel,:]
        dem_final = pd.DataFrame(data=None,columns=yearls,
                                 index=regionMap['Country']).reset_index(drop=False)
        
        ccs_dem_trend = ccs_dem_fuel.loc[ccs_dem_fuel['subsector']==ifuel,:]
        ccs_dem_final = pd.DataFrame(data=None,columns=yearls,
                                 index=regionMap['Country']).reset_index(drop=False)
        ccs_dem_trend.index = ccs_dem_trend['region']
        
        ccs_dem_trend = ccs_dem_trend.reindex(index=dem_trend['region'],fill_value=0)
        ccs_dem_trend['region'] = ccs_dem_trend.index.tolist()
        ccs_dem_trend['subsector'] = ifuel
        
        eng_trend = eng_fuel.loc[eng_fuel['subsector']==ifuel,:]
        eng_final = pd.DataFrame(data=None,columns=yearls,
                                 index=regionMap['Country']).reset_index(drop=False)
        
        for ireg in dem_trend['region']:
            icountry = regionMap.loc[regionMap['GCAM_region']==ireg,'Country']
            dem_final.loc[np.isin(dem_final['Country'],icountry),yearls] = dem_trend.loc[dem_trend['region']==ireg,yearls].values
            ccs_dem_final.loc[np.isin(ccs_dem_final['Country'],icountry),yearls] = ccs_dem_trend.loc[ccs_dem_trend['region']==ireg,yearls].values

            icountry = regionMap.loc[regionMap['GCAM_region']==ireg,'Country']
            eng_final.loc[np.isin(eng_final['Country'],icountry),yearls] = eng_trend.loc[eng_trend['region']==ireg,yearls].values
        
        dem_final.loc[pd.isnull(dem_final[yearls[0]])==1,yearls] = dem_trend[yearls].sum().values
        ccs_dem_final.loc[pd.isnull(ccs_dem_final[yearls[0]])==1,yearls] = ccs_dem_trend[yearls].sum().values
        eng_final.loc[pd.isnull(eng_final[yearls[0]])==1,yearls] = eng_trend[yearls].sum().values
        
        dem_final.loc[:,yearls] = dem_final.loc[:,yearls].mask(dem_final.loc[:,yearls]==0,9999)
        ccs_dem_final.loc[:,yearls] = ccs_dem_final.loc[:,yearls].values/dem_final.loc[:,yearls].values
        
        pd_write_new(dem_out,['Power_'+ifuel],[dem_final])
        pd_write_new(eng_out,['Power_'+ifuel],[eng_final])
        pd_write_new(ccs_ratio_out,['Power_'+ifuel],[ccs_dem_final])
        
        if ifuel == 'Biomass':
            pd_write_new(dem_out,['Power_Other'],[dem_final])
            pd_write_new(eng_out,['Power_Other'],[eng_final])
            
    return

def cement_work(eng,dem):
    
    dem_final = pd.DataFrame(data=None,columns=yearls,
                             index=regionMap['Country']).reset_index(drop=False)
    ccs_dem_final = pd.DataFrame(data=None,columns=yearls,
                             index=regionMap['Country']).reset_index(drop=False)
    
    sinter_ratio = pd.read_excel('../input/dict/Clinker2Cement.xlsx')
    
    dem_trend = dem.groupby(['subsector','region'],as_index=False).sum()
    
    ccs_dem_trend = dem.loc[dem['technology'].str.contains('CCS'),:].groupby(['subsector','region'],as_index=False).sum()
    ccs_dem_trend.index = ccs_dem_trend['region']
    ccs_dem_trend = ccs_dem_trend.reindex(index=dem_trend['region'],fill_value=0)
    ccs_dem_trend['region'] = ccs_dem_trend.index.tolist()
    ccs_dem_trend['subsector'] = 'cement'
    
    for ireg in dem['region']:
        icountry = regionMap.loc[regionMap['GCAM_region']==ireg,'Country']
        dem_final.loc[np.isin(dem_final['Country'],icountry),yearls] = dem_trend.loc[dem_trend['region']==ireg,yearls].values
        ccs_dem_final.loc[np.isin(dem_final['Country'],icountry),yearls] = ccs_dem_trend.loc[ccs_dem_trend['region']==ireg,yearls].values
        
    dem_final.loc[pd.isnull(dem_final[yearls[0]])==1,yearls] = dem_trend[yearls].sum().values
    ccs_dem_final.loc[pd.isnull(ccs_dem_final[yearls[0]])==1,yearls] = ccs_dem_trend[yearls].sum().values  
    
    dem_final.loc[:,yearls] = dem_final.loc[:,yearls].mask(dem_final.loc[:,yearls]==0,9999)
    ccs_dem_final.loc[:,yearls] = ccs_dem_final.loc[:,yearls].values/dem_final.loc[:,yearls].values
    pd_write_new(ccs_ratio_out,['Clinker'],[ccs_dem_final])
        
    sin_final = dem_final.copy(deep=True)
    sin_final.loc[:,yearls] = sin_final.loc[:,yearls]*sinter_ratio.loc[:,yearls]
    
    pd_write_new(dem_out,['Clinker','Cement'],[sin_final,dem_final])

    fuelmap = pd.read_excel('../input/dict/FuelMapping.xlsx',sheet_name='Cement')
    fuel_out = ['Coal', 'Gas', 'Oil','Biomass']

    eng_fuel = eng.groupby(['input','region'],as_index=False).sum()
    eng_fuel.replace(fuelmap['fuel_gcam'].values,fuelmap['fuel_gid'].values,inplace=True)
    eng_fuel = eng_fuel.loc[np.isin(eng_fuel['input'],fuel_out)]
    
    for ifuel in fuel_out:
        eng_trend = eng_fuel.loc[eng_fuel['input']==ifuel,:]
        eng_final = pd.DataFrame(data=None,columns=yearls,
                                 index=regionMap['Country']).reset_index(drop=False)
        
        for ireg in eng_trend['region']:
            icountry = regionMap.loc[regionMap['GCAM_region']==ireg,'Country']
            eng_final.loc[np.isin(eng_final['Country'],icountry),yearls] = eng_trend.loc[eng_trend['region']==ireg,yearls].values
            

        eng_final = eng_final.fillna(0)
        eng_final.loc[eng_final[yearls[0]]==0,yearls] = eng_trend[yearls].sum().values
        
        pd_write_new(eng_out,['Cement_'+ifuel],[eng_final])
        
    return

def IronAndSteel_work(eng,dem):
    iron_final = pd.DataFrame(data=None,columns=yearls,
                             index=regionMap['Country']).reset_index(drop=False)
    steel_final = pd.DataFrame(data=None,columns=yearls,
                             index=regionMap['Country']).reset_index(drop=False)
    iron_ccs_dem_final = pd.DataFrame(data=None,columns=yearls,
                             index=regionMap['Country']).reset_index(drop=False)
    steel_ccs_dem_final = pd.DataFrame(data=None,columns=yearls,
                             index=regionMap['Country']).reset_index(drop=False)
    
    dem_fuel = dem.groupby(['subsector','region'],as_index=False).sum()
    
    iron_fuel = dem_fuel.loc[~dem_fuel['subsector'].isin(['EAF with scrap']),:].groupby(['region'],as_index=False).sum()
    
    iron_ccs_dem_trend = dem.loc[(dem['technology'].str.contains('CCS')==1)&(dem['technology'].str.contains('EAF with scrap')==0),:]
    iron_ccs_dem_trend = iron_ccs_dem_trend.groupby(['region'],as_index=False).sum()
    
    iron_ccs_dem_trend.index = iron_ccs_dem_trend['region']
    iron_ccs_dem_trend = iron_ccs_dem_trend.reindex(index=iron_fuel['region'],fill_value=0)
    iron_ccs_dem_trend['region'] = iron_ccs_dem_trend.index.tolist()
    #ccs_dem_trend['subsector'] = 'cement'
    
    steel_fuel = dem_fuel.groupby(['region'],as_index=False).sum()
    
    steel_ccs_dem_trend = dem.loc[(dem['technology'].str.contains('CCS')==1),:]
    steel_ccs_dem_trend = steel_ccs_dem_trend.groupby(['region'],as_index=False).sum()
    
    steel_ccs_dem_trend.index = steel_ccs_dem_trend['region']
    steel_ccs_dem_trend = steel_ccs_dem_trend.reindex(index=iron_fuel['region'],fill_value=0)
    steel_ccs_dem_trend['region'] = steel_ccs_dem_trend.index.tolist()
    
    for ireg in dem['region']:
        icountry = regionMap.loc[regionMap['GCAM_region']==ireg,'Country']
        iron_final.loc[np.isin(iron_final['Country'],icountry),yearls] = iron_fuel.loc[iron_fuel['region']==ireg,yearls].values
        steel_final.loc[np.isin(steel_final['Country'],icountry),yearls] = steel_fuel.loc[steel_fuel['region']==ireg,yearls].values
        iron_ccs_dem_final.loc[np.isin(steel_final['Country'],icountry),yearls] = iron_ccs_dem_trend.loc[iron_ccs_dem_trend['region']==ireg,yearls].values
        steel_ccs_dem_final.loc[np.isin(steel_final['Country'],icountry),yearls] = steel_ccs_dem_trend.loc[steel_ccs_dem_trend['region']==ireg,yearls].values
        
    
    iron_final.loc[pd.isnull(iron_final[yearls[0]])==1,yearls] = iron_fuel[yearls].sum().values
    steel_final.loc[pd.isnull(steel_final[yearls[0]])==1,yearls] = steel_fuel[yearls].sum().values
    iron_ccs_dem_final.loc[pd.isnull(iron_ccs_dem_final[yearls[0]])==1,yearls] = iron_ccs_dem_trend[yearls].sum().values
    steel_ccs_dem_final.loc[pd.isnull(steel_ccs_dem_final[yearls[0]])==1,yearls] = steel_ccs_dem_trend[yearls].sum().values
    
    steel_final.loc[:,yearls] = steel_final.loc[:,yearls].mask(steel_final.loc[:,yearls]==0,9999)
    iron_final.loc[:,yearls] = iron_final.loc[:,yearls].mask(iron_final.loc[:,yearls]==0,9999)
    steel_ccs_dem_final.loc[:,yearls] = steel_ccs_dem_final.loc[:,yearls].values/steel_final.loc[:,yearls].values
    iron_ccs_dem_final.loc[:,yearls] = iron_ccs_dem_final.loc[:,yearls].values/iron_final.loc[:,yearls].values
    
    pd_write_new(dem_out,['Steel'],[steel_final])
    pd_write_new(ccs_ratio_out,['Steel'],[steel_ccs_dem_final])
    
    for iproc in ['Sinter','Pellet','Iron']:
        proc_ratio = pd.read_excel('../input/dict/Proc2Steel.xlsx',sheet_name='Ratio_'+iproc)
        proc_final= iron_final.copy(deep=True)
        proc_final.loc[:,yearls] = proc_ratio.loc[:,yearls]*proc_final.loc[:,yearls]
        
        proc_final = proc_final.fillna(0)
        pd_write_new(dem_out,[iproc],[proc_final])
        pd_write_new(ccs_ratio_out,[iproc],[iron_ccs_dem_final])
    
    fuelmap = pd.read_excel('../input/dict/FuelMapping.xlsx',sheet_name='IronAndSteel')
    fuel_out = ['Coal', 'Gas', 'Oil','Biomass']

    eng_fuel = eng.groupby(['input','region'],as_index=False).sum()
    eng_fuel.replace(fuelmap['fuel_gcam'].values,fuelmap['fuel_gid'].values,inplace=True)
    eng_fuel = eng_fuel.loc[np.isin(eng_fuel['input'],fuel_out)]
    
    for ifuel in fuel_out:
        eng_trend = eng_fuel.loc[eng_fuel['input']==ifuel,:]
        eng_final = pd.DataFrame(data=None,columns=yearls,
                                 index=regionMap['Country']).reset_index(drop=False)
        
        for ireg in eng_trend['region']:
            icountry = regionMap.loc[regionMap['GCAM_region']==ireg,'Country']
            eng_final.loc[np.isin(eng_final['Country'],icountry),yearls] = eng_trend.loc[eng_trend['region']==ireg,yearls].values
            
        eng_final = eng_final.fillna(0)
        eng_final.loc[eng_final[yearls[0]]==0,yearls] = eng_trend[yearls].sum().values
        
        pd_write_new(eng_out,['Iron&steel_'+ifuel],[eng_final])

    return

#%%
regionMap = pd.read_excel('../input/dict/RegionMapping.xlsx')

for isc in Eng_scenarios:
    eng_in = OUTPUT_PATH+'/'+isc+'/S1_BeforeMapping/pp_energy_scenario.xlsx'
    dem_in = OUTPUT_PATH+'/'+isc+'/S1_BeforeMapping/pp_demand_scenario.xlsx'
    
    mkdir(OUTPUT_PATH+'/'+isc+'/S2_Trend/')
    eng_out = OUTPUT_PATH+'/'+isc+'/S2_Trend/pp_energy_scenario.xlsx'
    dem_out = OUTPUT_PATH+'/'+isc+'/S2_Trend/pp_demand_scenario.xlsx'
    ccs_ratio_out = OUTPUT_PATH+'/'+isc+'/S2_Trend/pp_ccs_demand_scenario.xlsx'
    
    if os.path.exists(eng_out) == 1:
        os.remove(eng_out)
    if os.path.exists(dem_out) == 1:
        os.remove(dem_out)
    if os.path.exists(ccs_ratio_out) == 1:
        os.remove(ccs_ratio_out)
        
    eng_pow = pd.read_excel(eng_in,sheet_name='Power')
    dem_pow = pd.read_excel(dem_in,sheet_name='Power')
    power_work(eng=eng_pow,dem=dem_pow)
    
    eng_cem = pd.read_excel(eng_in,sheet_name='Cement')
    dem_cem = pd.read_excel(dem_in,sheet_name='Cement')
    cement_work(eng=eng_cem,dem=dem_cem)
    
    eng_iro = pd.read_excel(eng_in,sheet_name='IronAndSteel')
    dem_iro = pd.read_excel(dem_in,sheet_name='IronAndSteel')
    IronAndSteel_work(eng=eng_iro,dem=dem_iro)
    
    mkdir('../../5_PPTurnover/input/'+dir_prefix+'/'+isc+'/scenarios/')
    shutil.copyfile(eng_out, '../../5_PPTurnover/input/'+dir_prefix+'/'+isc+'/scenarios/'+'pp_energy_scenario.xlsx')
    shutil.copyfile(dem_out, '../../5_PPTurnover/input/'+dir_prefix+'/'+isc+'/scenarios/'+'pp_demand_scenario.xlsx')
    