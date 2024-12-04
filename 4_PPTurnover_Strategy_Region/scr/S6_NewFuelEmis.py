# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 10:11:35 2023

@author: yanxizhe
"""

#%%
import pandas as pd
import numpy as np
#import sys
from S1_Global_ENV import *
import time
import os
import multiprocessing

#%%
#新建文件夹
def mkdir(path):
	folder = os.path.exists(path)
	if not folder:
		os.makedirs(path)

#计算新建产能的能耗
def new_fuel_emis(isec,ifa,ireg,NewProdDistr_dir,NewFuelEmis_dir,yr_beg,yr_end):
    
    eff_dict = pd.read_excel('../input/dict/Dict_EnergyEfficiency.xlsx',sheet_name=isec)
    ener_eff = eff_dict.loc[eff_dict['Facility Type']==ifa,ireg].values[0]
    del eff_dict
    
    prod = pd.read_csv(NewProdDistr_dir+isec+'_'+ifa+'_'+ireg+'_NewProduction_PP_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    prod.drop(['Rank'],axis=1,inplace=True)
    final_df = prod.melt(id_vars=['Fake_Plant ID', 'Plant ID', 'Country','Start Year'],
                        var_name='Year', 
                        value_name='Production')
    final_df['Year'] = final_df['Year'].astype(int)
    
    ef_po_dict = pd.read_excel('../input/dict/Dict_EF_CO2_Process.xlsx',sheet_name=isec)
    ef_po = ef_po_dict.loc[ef_po_dict['Facility Type']==ifa,ireg].values[0]
    del ef_po_dict
    
    ef_co_dict = pd.read_excel('../input/dict/Dict_EF_CO2_Combustion.xlsx',sheet_name=isec)
    ef_co = ef_co_dict.loc[ef_co_dict['Facility Type']==ifa,ireg].values[0]
    del ef_co_dict
    
    final_df['Activity rates'] = final_df['Production']*ener_eff
    final_df['CO2 Emissions'] = final_df['Activity rates']*ef_co+final_df['Production']*ef_po
    
    pha_dict = pd.read_excel('../input/dict/DictOfPhaseout.xlsx',sheet_name='Max_Load_Life')
    unit_cap = pha_dict.loc[pha_dict['Sector']==isec,'Newbuilt_Capacity'].values[0]
    
    final_df = final_df.loc[final_df['Production']>0,:].reset_index(drop=True)
    if isec == 'Power':
        final_df['Production Unit'] = 'kWh'
        final_df['Activity type'] = 'Fuel consumption'
        final_df['Activity rates Unit'] = 'kt'
        final_df['Capacity'] = unit_cap*2
        final_df['Capacity Unit'] = 'MWh'
        
    elif isec == 'IronAndSteel':
        final_df['Production Unit'] = 'kt'
        final_df['Activity type'] = 'Energy Consumption'
        final_df['Activity rates Unit'] = 'GJ'
        final_df['Capacity'] = unit_cap*2
        final_df['Capacity Unit'] = 'Mt'
    
    elif isec == 'Cement':
        final_df['Production Unit'] = 'kt'
        final_df['Activity type'] = 'Energy Consumption'
        final_df['Activity rates Unit'] = 'GJ'
        final_df['Capacity'] = unit_cap*2
        final_df['Capacity Unit'] = 'Mt'
        
    final_df['Facility Type'] = ifa
    
    return final_df

def pp_combine(isec,ifa,ireg,
               OldProdEmis_dir,CCSInstall_dir,
               yr_beg,yr_end,new_pp):
    
    pp_file = pd.read_pickle('../../2_GetPPHarmonized/output/5_RegionPP/'+isec+'_'+ifa+'_'+ireg+'.pkl')
    col = ['Plant ID']+pp_file.columns[np.isin(pp_file.columns,new_pp.columns)==0].tolist()
    new_pp = pd.merge(new_pp,pp_file.loc[:,col],on='Plant ID',how='left')
    new_pp.loc[:,['Age','Fuel Type','Plant Name','Facility ID']] = np.nan
    new_pp['CO2 Eta (%)'] = 0
    
    old_pp = pd.read_csv(OldProdEmis_dir+isec+'_'+ifa+'_'+ireg+'_OldProdEmis_PP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',encoding='utf-8-sig')
    if yr_beg == startyr:
        old_pp['Fake_Plant ID'] = old_pp['Plant ID']
    else:
        GID_all = pd.read_csv(CCSInstall_dir+ireg+'_GIDAll_all.csv',encoding='utf-8-sig')
        GID_all = GID_all.loc[:,['Plant ID', 'Location_Plant ID']].drop_duplicates()
        old_pp = pd.merge(GID_all,old_pp,on='Plant ID',how='right')
        old_pp.rename(columns={'Plant ID':'Fake_Plant ID','Location_Plant ID':'Plant ID'},inplace=True)
        
    all_pp = pd.concat([new_pp,old_pp],axis=0)
    all_pp.reset_index(drop=True,inplace=True)
    
    return all_pp

#%%
def NewFuelEmis_main(ieng_sc,iend_sc,ior_sc,isec,ifa,ireg,
                     NewProdDistr_dir,OldProdEmis_dir,NewFuelEmis_dir,CCSInstall_dir,
                     yr_beg,yr_end):
    
    a = time.time()
    
    yearls2 = pd.Series(np.linspace(yr_beg,yr_end,int((yr_end-yr_beg)/gap)+1)).astype(int)
        
    new_pp = new_fuel_emis(isec,ifa,ireg,NewProdDistr_dir,NewFuelEmis_dir,yr_beg,yr_end)
    new_pp.to_csv(NewFuelEmis_dir+isec+'_'+ifa+'_'+ireg+'_NewProdEmis_PP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None,encoding='utf-8-sig')

    pp_this_period_coun = new_pp.loc[:,['Country','Year','Production','Activity rates','CO2 Emissions']].groupby(['Country','Year'],as_index=False).sum()
    pp_this_period_coun.to_csv(NewFuelEmis_dir+isec+'_'+ifa+'_'+ireg+'_NewProdEmis_Coun_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    all_pp = pp_combine(isec,ifa,ireg,
                        OldProdEmis_dir,CCSInstall_dir,
                        yr_beg,yr_end,new_pp)
    
    all_pp['CO2 Eta (%)'] = all_pp['CO2 Eta (%)'].fillna(0)
    all_pp.to_csv(NewFuelEmis_dir+isec+'_'+ifa+'_'+ireg+'_AllPP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None,encoding='utf-8-sig')

    #全国尺度的检验
    pp_this_period_coun = all_pp.loc[:,['Country','Year','Production','Activity rates','CO2 Emissions']].groupby(['Country','Year'],as_index=False).sum()
    pp_this_period_coun.to_csv(NewFuelEmis_dir+isec+'_'+ifa+'_'+ireg+'_AllCoun_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    ############################全部点源排放/产量数据
    
    
    return

#%%
if __name__ == '__main__':
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Age'
    isec='Cement'
    ifa='Clinker'
    ireg='Russia+Eastern Europe'
    
    NewProdDistr_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/1_NewBuilt/'
    OldProdEmis_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/2_OldProdEmis/'
    NewFuelEmis_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/3_NewFuelEmis/'
    CCSInstall_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/6_CCSInstall/'
    
    mkdir(NewFuelEmis_dir)
    
    yr_beg=2020
    yr_end=2030
    
    NewFuelEmis_main(ieng_sc,iend_sc,ior_sc,isec,ifa,ireg,
                     NewProdDistr_dir,OldProdEmis_dir,NewFuelEmis_dir,CCSInstall_dir,
                     yr_beg,yr_end)