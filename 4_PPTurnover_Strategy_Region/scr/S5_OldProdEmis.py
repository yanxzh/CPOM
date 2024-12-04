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

#%%
def Old_ProdEmis(isec,ifa,ireg,pp,
                 Turnover_dir,yr_beg,yr_end,yearls2):
    
    old_cf = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OldUnitUsage_PP_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    old_cf = old_cf.loc[:,['Plant ID']+yearls2.astype(str).tolist()]
    
    scale_cf = old_cf.copy(deep=True)
    for iyr in yearls2[1:]:
        scale_cf[str(iyr)] = scale_cf[str(iyr)].values/scale_cf[str(yr_beg)].values
    scale_cf[str(yr_beg)] = 1
    
    scale_cf = scale_cf.melt(id_vars=['Plant ID'],
                                var_name='Year', 
                                value_name='CF_SF')
    scale_cf['CF_SF'] = scale_cf['CF_SF'].astype(float)
    scale_cf['Year'] = scale_cf['Year'].astype(int)
    scale_cf = scale_cf.loc[scale_cf['CF_SF']>0,:].reset_index(drop=True)
    
    pp.drop(['Year'],axis=1,inplace=True)
    final_df = pd.merge(pp,scale_cf,on='Plant ID',how='right')
    
    final_df['Production'] = final_df['Production']*final_df['CF_SF']
    final_df['CO2 Emissions'] = final_df['CO2 Emissions']*final_df['CF_SF']
    final_df['Activity rates'] = final_df['Activity rates']*final_df['CF_SF']
    final_df.drop(['CF_SF'],axis=1,inplace=True)
    
    return final_df

#%%
#测试使用
def Old_ProdEmis_main(ieng_sc,iend_sc,ior_sc,isec,ifa,ireg,Turnover_dir,OldProdEmis_dir,CCSInstall_dir,yr_beg,yr_end):
    
    #阶段性的年份信息
    yearls2 = pd.Series(np.linspace(yr_beg,yr_end,int((yr_end-yr_beg)/gap)+1)).astype(int)
    
    a = time.time()
    
    if yr_beg == startyr:
        pp_file = pd.read_pickle('../../2_GetPPHarmonized/output/5_RegionPP/'+isec+'_'+ifa+'_'+ireg+'.pkl')
        pp_file = pp_file.sort_values(['Plant ID']).reset_index(drop=True)
        #pp_file = pp_file.loc[pp_file['Production']>0,:]
    else:
        pp_file = pd.read_csv(CCSInstall_dir+ireg+'_GIDAll_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv',encoding='utf-8-sig')
        pp_file = pp_file.loc[(pp_file['Year']==yr_beg)&(pp_file['Sector']==isec)&(pp_file['Facility Type']==ifa),:].reset_index(drop=True)
        pp_file.drop(['Location_Plant ID'],axis=1,inplace=True)
    
    pp_this_period = Old_ProdEmis(isec=isec,ifa=ifa,ireg=ireg,
                                    pp=pp_file,Turnover_dir=Turnover_dir,
                                    yr_beg=yr_beg,yr_end=yr_end,yearls2=yearls2)
    pp_this_period.to_csv(OldProdEmis_dir+isec+'_'+ifa+'_'+ireg+'_OldProdEmis_PP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None,encoding='utf-8-sig')

    #全国尺度的检验
    pp_this_period_coun = pp_this_period.loc[:,['Country','Year','Production','Activity rates','CO2 Emissions']].groupby(['Country','Year'],as_index=False).sum()
    pp_this_period_coun.to_csv(OldProdEmis_dir+isec+'_'+ifa+'_'+ireg+'_OldProdEmis_Coun_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    b = time.time()
            
    return

#%%
if __name__ == '__main__':
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Age'
    isec='Power'
    ifa='Coal'
    ireg='Russia+Eastern Europe'
    Turnover_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/0_Turnover/'
    OldProdEmis_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/2_OldProdEmis/'
    CCSInstall_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/6_CCSInstall/'
    
    mkdir(OldProdEmis_dir)
    mkdir(CCSInstall_dir)
    
    yr_beg=2030
    yr_end=2040
    
    Old_ProdEmis_main(ieng_sc,iend_sc,ior_sc,isec,ifa,ireg,Turnover_dir,OldProdEmis_dir,CCSInstall_dir,yr_beg,yr_end)