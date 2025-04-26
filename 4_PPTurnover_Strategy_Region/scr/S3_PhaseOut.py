# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 10:11:35 2023

@author: yanxizhe
"""

#%%
import pandas as pd
import numpy as np
from S1_Global_ENV import *
import time
import os

#%%        
def phase_out_age(pp,sec,ireg,Turnover_dir,engsc,fa,ordsc,yr_ls,yr_beg,yr_end):
    op = pd.DataFrame(data=None,index=pp['Plant ID'],columns=pd.Series(yr_ls))
    op = op.reset_index(drop=False).sort_values(['Plant ID']).reset_index(drop=True)

    if yr_beg == startyr:
        pha_order = pd.read_csv('../../2_GetPPHarmonized/output/3_AgeRank/'+sec+'_'+fa+'.csv')
        pha_order = pha_order.loc[:,['Plant ID','Age rank']]
        pha_order.rename(columns={'Age rank':'Order'},inplace=True)
        pp = pd.merge(pp,pha_order,on='Plant ID',how='left')
    else:
        pha_order = pd.read_csv(Turnover_dir+sec+'_'+fa+'_'+ireg+'_AgeRank_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv')
        pha_order.drop(['Age'],axis=1,inplace=True)
        pp = pd.merge(pp,pha_order,on='Plant ID',how='left')

    pha_dict = pd.read_excel('../input/dict/DictOfPhaseout.xlsx',sheet_name='Max_Load_Life')

    if uncer_test == 0:
        max_life = pha_dict.loc[pha_dict['Sector']==sec,'LifeTime'].values[0]
    elif uncer_test == 1:
        max_life = pha_dict.loc[pha_dict['Sector']==sec,'LifeTime'].values[0]
        normal_ran = np.random.normal(loc=1,scale=0.05,size=1)[0]
        max_life = int(max_life * normal_ran)

    pp['Start Year'] = pp['Start Year'].astype(int)

    if yr_ls[0] == startyr:
        old_pp = pp.loc[startyr-pp['Start Year']>=max_life,['Plant ID','Country','Order','Capacity']]
        old_pp = old_pp.sort_values(['Country','Order']).reset_index(drop=True)
        old_pp.insert(loc=old_pp.shape[1],column='CapaCumSum',value=old_pp.groupby(['Country'])['Capacity'].cumsum())
        old_cap = old_pp.groupby(['Country'],as_index=False).sum().loc[:,['Country','Capacity']]
        phase_in_time = pha_dict.loc[pha_dict['Sector']==sec,'OverAgedPhaseOutInYear'].values[0]

        if yr_ls[0] == startyr:
            for iyr in yr_ls[1:phase_in_time+1]:
                pha_cap = pd.DataFrame((old_cap['Capacity']/phase_in_time*(iyr-startyr)).values,columns=['PhaCapa'])
                pha_cap.insert(0,'Country',old_cap['Country'])
                pha_pp = pd.merge(old_pp,pha_cap,on='Country',how='outer')
                phase_ID = pha_pp.loc[(pha_pp['CapaCumSum']<=pha_pp['PhaCapa']*(1+1/10000)),'Plant ID']
                op.loc[np.isin(op['Plant ID'],phase_ID),yr_ls[int(iyr-yr_ls[0]):]] = 0

    phaseout_year = pd.DataFrame(data=(pp['Start Year']+max_life).values,columns=['PhaYr'])
    phaseout_year.loc[pp['CO2 Eta (%)']==90,'PhaYr'] = phaseout_year.loc[pp['CO2 Eta (%)']==90,'PhaYr']+extend_life
    phaseout_year.loc[phaseout_year['PhaYr']<=startyr,'PhaYr'] = 9999
    phaseout_year.insert(0,'Plant ID',pp['Plant ID'].values)

    for iyr in range(yr_ls.shape[0]):
        phase_ID = phaseout_year.loc[(phaseout_year['PhaYr']==yr_ls[iyr]),'Plant ID']
        op.loc[np.isin(op['Plant ID'],phase_ID),yr_ls[iyr:]] = 0

    op.loc[:,yr_ls] = op.loc[:,yr_ls].mask(pd.isnull(op.loc[:,yr_ls])==1,1)
    op = pd.merge(pp.loc[:,['Plant ID','Order','Age','Country']],op,on='Plant ID',how='right')

    return op

def phaseout_main(ieng_sc,iend_sc,ior_sc,isec,ifa,ireg,Turnover_dir,CCSInstall_dir,yr_beg,yr_end):
    yearls2 = pd.Series(np.linspace(yr_beg,yr_end,int((yr_end-yr_beg)/gap)+1)).astype(int)

    if yr_beg == startyr:
        pp_file = pd.read_pickle('../../2_GetPPHarmonized/output/5_RegionPP/'+isec+'_'+ifa+'_'+ireg+'.pkl')
        pp_file = pp_file.sort_values(['Plant ID']).reset_index(drop=True)
    else:
        pp_file = pd.read_csv(CCSInstall_dir+ireg+'_GIDAll_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv',encoding='utf-8-sig')
        pp_file = pp_file.loc[(pp_file['Year']==yr_beg)&(pp_file['Sector']==isec)&(pp_file['Facility Type']==ifa),:].reset_index(drop=True)
        pp_file.drop(['Location_Plant ID'],axis=1,inplace=True)

    pp_file['Age'] = pp_file['Year']-pp_file['Start Year']

    production_trend = pd.read_csv('../../2_GetPPHarmonized/output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_RegionTrend.csv')
    production_trend = production_trend.loc[production_trend['Region']==ireg,['Region']+[str(iyr) for iyr in yearls2]].reset_index(drop=True)
    col_change = {'Region':'Country'}
    col_change.update({str(iyr):iyr for iyr in yearls2})
    production_trend.rename(columns=col_change,inplace=True)

    if ior_sc in ['Age','Cost','Emis']:
        operating_staus = phase_out_age(sec=isec,fa=ifa,ireg=ireg,
                                        Turnover_dir=Turnover_dir,pp=pp_file,
                                        engsc=ieng_sc,ordsc=ior_sc,
                                        yr_ls=yearls2,yr_beg=yr_beg,yr_end=yr_end)

    operating_staus.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    production_trend.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_ProductionTrend_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)

    return

#%%
def phaseout_main(ieng_sc,iend_sc,ior_sc,isec,ifa,ireg,Turnover_dir,CCSInstall_dir,yr_beg,yr_end):
    yearls2 = pd.Series(np.linspace(yr_beg,yr_end,int((yr_end-yr_beg)/gap)+1)).astype(int)

    if yr_beg == startyr:
        pp_file = pd.read_pickle('../../2_GetPPHarmonized/output/5_RegionPP/'+isec+'_'+ifa+'_'+ireg+'.pkl')
        pp_file = pp_file.sort_values(['Plant ID']).reset_index(drop=True)
    else:
        pp_file = pd.read_csv(CCSInstall_dir+ireg+'_GIDAll_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv',encoding='utf-8-sig')
        pp_file = pp_file.loc[(pp_file['Year']==yr_beg)&(pp_file['Sector']==isec)&(pp_file['Facility Type']==ifa),:].reset_index(drop=True)
        pp_file.drop(['Location_Plant ID'],axis=1,inplace=True)

    pp_file['Age'] = pp_file['Year']-pp_file['Start Year']

    production_trend = pd.read_csv('../../2_GetPPHarmonized/output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_RegionTrend.csv')
    production_trend = production_trend.loc[production_trend['Region']==ireg,['Region']+[str(iyr) for iyr in yearls2]].reset_index(drop=True)
    col_change = {'Region':'Country'}
    col_change.update({str(iyr):iyr for iyr in yearls2})
    production_trend.rename(columns=col_change,inplace=True)

    if ior_sc in ['Age','Cost','Emis']:
        operating_staus = phase_out_age(sec=isec,fa=ifa,ireg=ireg,
                                        Turnover_dir=Turnover_dir,pp=pp_file,
                                        engsc=ieng_sc,ordsc=ior_sc,
                                        yr_ls=yearls2,yr_beg=yr_beg,yr_end=yr_end)

    operating_staus.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    production_trend.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_ProductionTrend_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    return

#%%
if __name__ == '__main__':
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Cost'
    isec='Cement'
    ifa='Clinker'
    ireg='China'
    Turnover_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/0_Turnover/'
    CCSInstall_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/6_CCSInstall/'
    
    yr_beg=2030
    yr_end=2040
    
    mkdir(Turnover_dir)
    mkdir(CCSInstall_dir)
    
    phaseout_main(ieng_sc,iend_sc,ior_sc,isec,ifa,ireg,Turnover_dir,CCSInstall_dir,yr_beg,yr_end)