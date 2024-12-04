# -*- coding: utf-8 -*-
"""
Created on Sat Mar 30 20:49:42 2024

@author: 92978
"""

#%%
import pandas as pd
import numpy as np
import shutil
import os
from S0_GlobalENV import *

#%%
def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)    

def prod_trend(sec,fa,pp,engsc):
    pp.loc[pp['Country']=='Taiwan, China','Country'] = 'Chinese Taipei'
    pp.loc[pp['Country']=='Macao, China','Country'] = 'Macao'
    
    if sec=='Power':
        gc_dem = pd.read_excel('../../1_GCAMScenario/output/Phaseout_CoalPower/'+engsc+'/S2_Trend/pp_demand_scenario.xlsx',sheet_name=sec+'_'+fa)
    else:
        gc_dem = pd.read_excel('../../1_GCAMScenario/output/Phaseout_CoalPower/'+engsc+'/S2_Trend/pp_demand_scenario.xlsx',sheet_name=fa)
    
    pp_country = pp.loc[:,['Country','Production']].groupby(['Country'],as_index=False).sum()
    pp_country = pd.merge(gc_dem['Country'],pp_country,on='Country',how='outer')
    
    gc_ratio = gc_dem.loc[:,yearls].div(gc_dem.loc[:,yearls[0]],axis=0)
    
    final_trend = gc_ratio.mul(pp_country['Production'].values,axis=0)
    final_trend = final_trend.fillna(value=0)
    final_trend.insert(loc=0,column='Country',value=gc_dem['Country'])
    
    return final_trend
    
def ccus_trend(sec,fa,df,engsc):
    if sec=='Power':
        if fa == 'Other':
            gc_dem = pd.read_excel('../../1_GCAMScenario/output/Phaseout_CoalPower/'+engsc+'/S2_Trend/pp_ccs_demand_scenario.xlsx',sheet_name='Power_Biomass')
        else:
            gc_dem = pd.read_excel('../../1_GCAMScenario/output/Phaseout_CoalPower/'+engsc+'/S2_Trend/pp_ccs_demand_scenario.xlsx',sheet_name=sec+'_'+fa)
    else:
        gc_dem = pd.read_excel('../../1_GCAMScenario/output/Phaseout_CoalPower/'+engsc+'/S2_Trend/pp_ccs_demand_scenario.xlsx',sheet_name=fa)
    
    df = pd.merge(gc_dem['Country'],df,on='Country',how='left')
    df.loc[:,yearls] = df.loc[:,yearls]*gc_dem.loc[:,yearls].values
    df = df.fillna(value=0)

    return df

#%%
reg_map = pd.read_excel('../../1_GCAMScenario/input/dict/RegionMapping.xlsx',
                        usecols=['Country','Region_CCUS'])

for ieng_sc in Eng_scenarios:
    mkdir('../output/3_HarmonizedTrend/'+ieng_sc)
    for isec in ['Power','Cement','IronAndSteel']:
        faciliy = fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']
        
        for ifa in faciliy:
            pp_file = pd.read_pickle('../output/2_PP_cut/'+isec+'_'+ifa+'.pkl')
            
            harmo_trend = prod_trend(sec=isec,fa=ifa,pp=pp_file,engsc=ieng_sc)
            harmo_trend.to_csv('../output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_CountryTrend.csv',index=None)
            
            ccs_trend = ccus_trend(sec=isec,fa=ifa,df=harmo_trend,engsc=ieng_sc)
            ccs_trend.to_csv('../output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_CCS_CountryTrend.csv',index=None)
            
            harmo_trend['Region'] = harmo_trend['Country'].replace(reg_map['Country'].values,reg_map['Region_CCUS'].values)
            harmo_trend.drop(['Country'],axis=1,inplace=True)
            
            harmo_trend = harmo_trend.groupby(['Region'],as_index=False).sum()
            harmo_trend.to_csv('../output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_RegionTrend.csv',index=None)
            
            ccs_trend['Region'] = ccs_trend['Country'].replace(reg_map['Country'].values,reg_map['Region_CCUS'].values)
            ccs_trend.drop(['Country'],axis=1,inplace=True)
            
            ccs_trend = ccs_trend.groupby(['Region'],as_index=False).sum()
            ccs_trend.to_csv('../output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_CCS_RegionTrend.csv',index=None)