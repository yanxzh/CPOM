# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 10:11:35 2023

@author: yanxizhe
使用熟料作为参考数据开发淘汰模型
这里的情景数据中有些国家基准年为0，我将这些国家的情景数据使用全球未来情景的趋势来替代
注意每次sort最好还是reset_index，Series加减乘除是根据index的
"""

#%%
import pandas as pd
import numpy as np
#import sys
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
        pp = pd.merge(pp,pha_order,on='Plant ID',how='left')#加入排序列
    else:
        pha_order = pd.read_csv(Turnover_dir+sec+'_'+fa+'_'+ireg+'_AgeRank_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv')
        pha_order.drop(['Age'],axis=1,inplace=True)
        pp = pd.merge(pp,pha_order,on='Plant ID',how='left')#加入排序列
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
                pha_cap.insert(loc=0,column='Country',value=old_cap['Country'])
                pha_pp = pd.merge(old_pp,pha_cap,on='Country',how='outer')
                
                phase_ID = pha_pp.loc[(pha_pp['CapaCumSum']<=pha_pp['PhaCapa']*(1+1/10000)),'Plant ID']
                op.loc[np.isin(op['Plant ID'],phase_ID),yr_ls[int(iyr-yr_ls[0]):]] = 0
                #op.loc[np.isin(op['Plant ID'],phase_ID),yearls[:int(iyr-startyr)]] = 1

    phaseout_year = pd.DataFrame(data=(pp['Start Year']+max_life).values,columns=['PhaYr'])
    phaseout_year.loc[pp['CO2 Eta (%)']==90,'PhaYr'] = phaseout_year.loc[pp['CO2 Eta (%)']==90,'PhaYr']+15
    phaseout_year.loc[phaseout_year['PhaYr']<=startyr,'PhaYr'] = 9999
    phaseout_year.insert(loc=0,column='Plant ID',value=pp['Plant ID'].values)
    
    for iyr in range(yr_ls.shape[0]):
        phase_ID = phaseout_year.loc[(phaseout_year['PhaYr']==yr_ls[iyr]),'Plant ID']
        op.loc[np.isin(op['Plant ID'],phase_ID),yr_ls[iyr:]] = 0
        #op.loc[np.isin(op['Plant ID'],phase_ID),yearls[:iyr]] = 1
    
    op.loc[:,yr_ls] = op.loc[:,yr_ls].mask(pd.isnull(op.loc[:,yr_ls])==1,1)
    op = pd.merge(pp.loc[:,['Plant ID','Order','Age','Country']],op,on='Plant ID',how='right')
    
    return op

def new_prod_get(sec,pp,tre,op_status,yr_ls):
    pp_columns = ['Plant ID', 'Country', 'Capacity', 'Production']
    pp_base = pp.loc[:,pp_columns]
    pp_base = pp_base.sort_values(['Plant ID']).reset_index(drop=True)
    
    load = pp_base['Production']/pp_base['Capacity']/10**3
    pha_dict = pd.read_excel('../input/dict/DictOfPhaseout.xlsx',sheet_name='Max_Load_Life')
    new_method = pha_dict.loc[pha_dict.loc[:,'Sector']==sec,'CFMethod'].values[0]
    
    if new_method == 'AsUsual':
        max_load = load
    elif new_method == 'MaxLimit':
        max_load = pha_dict.loc[pha_dict['Sector']==sec,'CFMax'].values[0]
    elif new_method == 'Decrease':
        max_load = load
        decrease_rate = pha_dict.loc[pha_dict['Sector']==sec,'CFMax'].values[0]
    
    old_prod = op_status.copy(deep=True).reset_index(drop=True)
    # old_prod.insert(loc=1,column='Country',value=pp_base['Country'])
    old_prod.loc[:,yr_ls[0]:] = 0
    old_usage = old_prod.copy(deep=True)
    old_prod.loc[:,yr_ls[0]] = pp_base['Production']
    old_usage.loc[:,yr_ls[0]] = load
    
    new_prod = tre.copy(deep=True)
    new_prod.iloc[:,1:] = 0
    
    for iyr in yr_ls[1:]:
        yr_tre = tre.loc[:,['Country',iyr]]
        
        yr_op = op_status.loc[:,iyr]
        yr_act = old_prod.loc[:,iyr-1]*yr_op
        if new_method != 'Decrease':
            yr_maxact = pp_base['Capacity']*yr_op*1000*max_load
        else:
            if iyr == yr_ls[1]:
                yr_maxact = pp_base['Capacity']*yr_op*1000*max_load*(1-decrease_rate)**(iyr-yr_ls[0])
            else:
                yr_maxact = pp_base['Capacity']*yr_op*1000*old_usage[iyr-1]*(1-decrease_rate)**(iyr-yr_ls[0])
            
        yr_act_df = pd.concat([yr_maxact,yr_act],axis=1)
        yr_act_df.columns=['MaxAct','UsualAct']
        yr_act_df.insert(loc=0,column='Country',value=pp_base['Country'])
        yr_coun_act = yr_act_df.groupby(['Country'],as_index=False).sum()
        yr_coun_act = pd.merge(yr_coun_act, yr_tre,on='Country',how='inner')
        
        coun_ls_one = yr_coun_act.loc[yr_coun_act['UsualAct']>yr_coun_act[iyr],'Country']
        for icoun in coun_ls_one:
            scale_factor = yr_coun_act.loc[yr_coun_act['Country']==icoun,iyr]/yr_coun_act.loc[yr_coun_act['Country']==icoun,'UsualAct']

            flitered_usage1 = (old_prod['Country']==icoun)&(yr_act_df['UsualAct']>0)
            old_prod.loc[flitered_usage1,iyr] = old_prod.loc[flitered_usage1,iyr-1]*scale_factor.values
            old_usage.loc[flitered_usage1,iyr] = old_usage.loc[flitered_usage1,iyr-1]*scale_factor.values
            flitered_usage1 = (old_prod['Country']==icoun)&(yr_act_df['UsualAct']==0)
            old_prod.loc[flitered_usage1,iyr] = 0
            old_usage.loc[flitered_usage1,iyr] = 0       
        
        coun_ls_two = yr_coun_act.loc[yr_coun_act['MaxAct']<yr_coun_act[iyr],'Country']
        old_prod.loc[np.isin(old_prod['Country'],coun_ls_two),iyr] = yr_act_df.loc[np.isin(yr_act_df['Country'],coun_ls_two),'MaxAct']
        old_usage.loc[np.isin(old_usage['Country'],coun_ls_two),iyr] = \
            yr_act_df.loc[np.isin(yr_act_df['Country'],coun_ls_two),'MaxAct']/pp_base.loc[np.isin(pp_base['Country'],coun_ls_two),'Capacity']/1000
        
        for icoun in coun_ls_two:
            new_prod.loc[new_prod['Country']==icoun,iyr] = \
                (yr_coun_act.loc[yr_coun_act['Country']==icoun,iyr]-yr_coun_act.loc[yr_coun_act['Country']==icoun,'MaxAct']).values
        
        coun_ls_three = yr_coun_act.loc[(yr_coun_act['MaxAct']>yr_coun_act[iyr])&(yr_coun_act['UsualAct']<yr_coun_act[iyr]),'Country']
        for icoun in coun_ls_three:
            scale_factor = yr_coun_act.loc[yr_coun_act['Country']==icoun,iyr]/yr_coun_act.loc[yr_coun_act['Country']==icoun,'UsualAct']
            prod_change = old_prod.loc[old_prod['Country']==icoun,iyr-1]*scale_factor.values
            prod_change[yr_act_df.loc[yr_act_df['Country']==icoun,'UsualAct']==0] = 0 #把今年被淘汰掉的机组找出来
            
            full = []
            while np.where(prod_change>yr_act_df.loc[yr_act_df['Country']==icoun,'MaxAct'])[0].size>0:
                over_loc = prod_change.index[np.where(prod_change>yr_act_df.loc[yr_act_df['Country']==icoun,'MaxAct'])[0]]
                full = full+over_loc.tolist()
                over_sum = np.sum(prod_change[over_loc] - yr_act_df.loc[over_loc,'MaxAct'])
                
                rest_index = prod_change[np.isin(prod_change.index,full)==0].index
                #print(icoun)
                if sum(prod_change[rest_index])>0:
                    sf = over_sum/sum(prod_change[np.isin(prod_change.index,full)==0])
                    prod_change[np.isin(prod_change.index,full)==0] = prod_change[np.isin(prod_change.index,full)==0] * (1+sf)
                    prod_change[over_loc] = yr_act_df.loc[over_loc,'MaxAct']
                else:
                    sf = over_sum/sum(yr_act_df.loc[rest_index,'MaxAct'])
                    prod_change[np.isin(prod_change.index,full)==0] = yr_act_df.loc[rest_index,'MaxAct'] * sf
                    prod_change[over_loc] = yr_act_df.loc[over_loc,'MaxAct']
            
            old_prod.loc[old_prod['Country']==icoun,iyr] = prod_change
            flitered_usage1 = (old_usage['Country']==icoun)&(old_prod.loc[:,iyr]>0)
            old_usage.loc[flitered_usage1,iyr] = prod_change[flitered_usage1]/pp_base.loc[flitered_usage1,'Capacity']/1000
            flitered_usage2 = (old_usage['Country']==icoun)&(old_prod.loc[:,iyr]==0)
            old_usage.loc[flitered_usage2,iyr] = 0
                
    return old_prod,old_usage,new_prod

def op_combine(isec,ifa,ireg,
               Turnover_dir,
               yr_beg,yr_end):
    
    if yr_beg == startyr+10:
        op_ori = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv')
        op_ori.drop(['Order','Age',str(yr_end-10)],axis=1,inplace=True)
    else:
        op_ori = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_all.csv')
        op_ori.drop([str(yr_end-10)],axis=1,inplace=True)
    
    op_this = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    op_this.drop(['Order','Age','Country'],axis=1,inplace=True)
    op_this = op_this.loc[op_this['Plant ID'].str.contains(str(yr_beg-10)+'_'+str(yr_end-10))==0,:].reset_index(drop=True)
    
    op_all = pd.merge(op_ori,op_this,on='Plant ID',how='outer')
    op_all = op_all.fillna(0)
    
    op_all.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_all.csv',index=None)
    
    return

#%%
def phaseout_main(ieng_sc,iend_sc,ior_sc,
                  isec,ifa,ireg,
                  Turnover_dir,CCSInstall_dir,
                  yr_beg,yr_end):
    
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
    else:
        operating_staus = phase_out_policy(sec=isec,pp=pp_file,engsc=ieng_sc,endsc=iend_sc,ordsc=ior_sc)
    
    OldUnitProduction,OldUnitUsage,NewUnitProduction, = new_prod_get(sec=isec,pp=pp_file,tre=production_trend,op_status=operating_staus,yr_ls=yearls2)
    
    # operating_staus.insert(loc=1,column='Country',value=pp_file['Country'].values)
    operating_staus.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    production_trend.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_ProductionTrend_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    OldUnitProduction.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OldUnitProduction_PP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    OldUnitUsage.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OldUnitUsage_PP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    NewUnitProduction.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_NewUnitProduction_Coun_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    OldUnitProduction.loc[:,yearls2[1:]] = OldUnitProduction.loc[:,yearls2[1:]].astype(float)
    OldUnitProduction_coun = OldUnitProduction.groupby(['Country'],as_index=False).sum()
    OldUnitProduction_coun = pd.merge(NewUnitProduction['Country'],OldUnitProduction_coun,on='Country',how='outer')[['Country']+yearls2.tolist()]
    OldUnitProduction_coun = OldUnitProduction_coun.fillna(value=0)
    OldUnitProduction_coun.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OldUnitProduction_Coun_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    if yr_beg != startyr:
        op_combine(isec=isec,ifa=ifa,ireg=ireg,
                   Turnover_dir=Turnover_dir,
                   yr_beg=yr_beg,yr_end=yr_end)
    
    b = time.time()
    
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