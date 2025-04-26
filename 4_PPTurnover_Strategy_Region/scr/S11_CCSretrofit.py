# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 18:47:52 2024

@author: 92978
"""

#%%
import pandas as pd
import numpy as np
from S1_Global_ENV import *
import time
import os
import geopandas as gpd

#%%
def constrain_get(ieng_sc,ireg,
                  Turnover_dir,CCSInstall_dir,SSM_dir,NewFuelEmis_dir,
                  yr_beg,yr_end,yearls2):
    
    reg_list = [
                'China',
                'Other-Asia-and-Pacific',
                'Russia+Eastern-Europe',
                'Middle-East-and-Africa',
                'Canada+Latin-America',
                'Western-Europe',
                'India',
                'East-Asia',
                'United-States'
                ]

    reg_list2 = [
                'China',
                'Other Asia and Pacific',
                'Russia+Eastern Europe',
                'Middle East and Africa',
                'Canada+Latin America',
                'Western Europe',
                'India',
                'East Asia',
                'United States'
                ]
    
    reg2 = pd.Series(reg_list)[pd.Series(reg_list2)==ireg].values[0]
    reg1 = ireg
            
    so_si = gpd.read_file('../../4_SSM/PreNet/PreNet_'+reg2+'/output/1_IsolatePoint/3_SelectedPoint_'+reg1+'.shp')
    so_si.loc[:,['Longitude','Latitude','CO2','Capacity','DSA','WaterResou','WaterOrigi']] = \
        so_si.loc[:,['Longitude','Latitude','CO2','Capacity','DSA','WaterResou','WaterOrigi']].astype(float)
        
    so_si = so_si.rename(columns={'ID':'Plant ID'})

    df_commit_all = pd.DataFrame()
    
    a = 0
    for isec in pp_run:
        for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
            dem_coun = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_ProductionTrend_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
            dem_coun = dem_coun.loc[dem_coun['Country']==ireg,['Country']+yearls2.astype(str).tolist()].reset_index(drop=True)
            
            ccs_dem_coun = pd.read_csv('../../2_GetPPHarmonized/output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_CCS_RegionTrend.csv')
            ccs_dem_coun = ccs_dem_coun.loc[ccs_dem_coun['Region']==ireg,['Region']+yearls2.astype(str).tolist()].reset_index(drop=True)
            ccs_dem_coun.rename(columns={'Region':'Country'},inplace=True)
            
            ccs_dem_coun.loc[ccs_dem_coun['Country']==ireg,yearls2.astype(str)] = \
                ccs_dem_coun.loc[ccs_dem_coun['Country']==ireg,yearls2.astype(str)].values/dem_coun.loc[dem_coun['Country']==ireg,yearls2.astype(str)].values
            ccs_dem_coun.to_csv(CCSInstall_dir+isec+'_'+ifa+'_'+ireg+'_CCSNeedRatio_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
            
            df_ = pd.read_csv(NewFuelEmis_dir+isec+'_'+ifa+'_'+ireg+'_AllPP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',encoding='utf-8-sig')
            col = ['Fake_Plant ID','Plant ID','Country','Year','CO2 Emissions',
                   'Sector','Facility Type', 'Longitude', 'Latitude',
                   'Start Year','Age','Capacity','Capacity Unit','CO2 Eta (%)']
            df_ = df_.loc[(df_['Year']==yr_end),col].reset_index(drop=True)
            df_ = df_.loc[df_['Fake_Plant ID'].isin(so_si['Plant ID']),:].reset_index(drop=True)
            df_.rename(columns={'Fake_Plant ID':'Plant ID','Plant ID':'Location_Plant ID'},inplace=True)
            
            pha_dict = pd.read_excel('../input/dict/DictOfPhaseout.xlsx',sheet_name='Max_Load_Life')
            max_life = pha_dict.loc[pha_dict['Sector']==isec,'LifeTime'].values[0]
            del pha_dict
            
            df_['Age'] = yr_end-df_['Start Year'].astype(float)
            df_['Commitment (Mt)'] = df_['CO2 Emissions']/((100-df_['CO2 Eta (%)'])/100)*(max_life+extend_life-df_['Age'])/10**6
            df_ = df_.loc[df_['Commitment (Mt)']>0,:].reset_index(drop=True)

            df_commit_all = pd.concat([df_commit_all,df_],axis=0)
            
            if a == 0:
                constain_all = ccs_dem_coun.copy(deep=True)
                constain_all.loc[constain_all['Country']==ireg,yearls2.astype(str)] = 0
            
            if yr_beg == startyr:
                constrain_en = df_['Commitment (Mt)'].sum()
                    
                constain = ccs_dem_coun.copy(deep=True)
                constain.loc[constain['Country']==ireg,yearls2.astype(str)] = ccs_dem_coun.loc[ccs_dem_coun['Country']==ireg,yearls2.astype(str)]*\
                                                                                constrain_en*0.9
                
                cap_record_last = []
   
            else:
                cap_record = pd.read_csv(SSM_dir+'/capture_sec_'+ireg+'_marker.csv')
                cap_record_last = cap_record.loc[(cap_record['CCUSInstall']==0)&(cap_record['Year']==yr_beg),'Plant ID']
                constrain_st = df_.loc[(np.isin(df_['Plant ID'],cap_record_last)==1),'Commitment (Mt)'].sum()
                constrain_en = df_['Commitment (Mt)'].sum()*ccs_dem_coun.loc[ccs_dem_coun['Country']==ireg,str(yr_end)].values[0]
                
                sf = (constrain_en-constrain_st)/(ccs_dem_coun.loc[ccs_dem_coun['Country']==ireg,str(yr_end)].values[0]-ccs_dem_coun.loc[ccs_dem_coun['Country']==ireg,str(yr_beg)].values[0])
                if np.isinf(sf) == 1:
                    sf = 0
                    
                constain = ccs_dem_coun.copy(deep=True)
                constain.loc[constain['Country']==ireg,str(yr_beg)] = constrain_st*0.9
                for iyr in yearls2[1:]:
                    constain.loc[constain['Country']==ireg,str(iyr)] = constain.loc[constain['Country']==ireg,str(iyr-1)]+\
                        sf*(ccs_dem_coun.loc[ccs_dem_coun['Country']==ireg,str(iyr)]-ccs_dem_coun.loc[ccs_dem_coun['Country']==ireg,str(iyr-1)])*0.9
            
            constain_all.loc[constain_all['Country']==ireg,yearls2.astype(str)] = constain.loc[constain['Country']==ireg,yearls2.astype(str)]+\
                                        constain_all.loc[constain_all['Country']==ireg,yearls2.astype(str)]
            
            a = a+1
    
    df_commit_all.to_csv(CCSInstall_dir+'/CommitmentAll_'+ireg+'_AllPP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    return constain_all,cap_record_last

def get_op(Turnover_dir,NewProdDistr_dir,CCSInstall_dir,
           ireg,yr_beg,yr_end):
    
    df_new = pd.DataFrame()
    df_old = pd.DataFrame()
    
    for isec in pp_run:
        for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
            old_op = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
            df_old = pd.concat([df_old,old_op],axis=0)
            
            new_op = pd.read_csv(NewProdDistr_dir+isec+'_'+ifa+'_'+ireg+'_NewPP_OperatingStatus_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
            df_new = pd.concat([df_new,new_op],axis=0)
    
    df_new.drop(['Rank'],axis=1,inplace=True)
    df_old.drop(['Order', 'Age'],axis=1,inplace=True)

    if yr_beg == startyr:
        df_old['Fake_Plant ID'] = df_old['Plant ID']
    else:
        GID_all = pd.read_csv(CCSInstall_dir+ireg+'_GIDAll_all.csv',encoding='utf-8-sig')
        GID_all = GID_all.loc[:,['Plant ID', 'Location_Plant ID']].drop_duplicates()
        df_old = pd.merge(GID_all,df_old,on='Plant ID',how='right')
        df_old.rename(columns={'Plant ID':'Fake_Plant ID','Location_Plant ID':'Plant ID'},inplace=True)
    
    df_all = pd.concat([df_new,df_old],axis=0)
    df_all.rename(columns={'Fake_Plant ID':'Plant ID','Plant ID':'Location_Plant ID'},inplace=True)
    
    return df_all

def CCS_install(ccs_constain,op_all,cap_record_last,
                SSM_dir,NetworkStatus_dir,Turnover_dir,CCSInstall_dir,
                ior_sc,ireg,yr_beg,yr_end,yearls2):
    
    early_id_all = pd.DataFrame(columns=['Year','Plant ID'])
    
    if yr_beg == startyr:
        cap_data = pd.read_csv(SSM_dir+'/capture_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
        if ior_sc == 'Cost':
            source_cost_rank = pd.read_csv(NetworkStatus_dir+'/4_CostAndFlow/2_Cost_rank_'+ireg+'.csv')
            
            cap_data = pd.merge(cap_data,source_cost_rank.loc[:,['Plant ID','Cost rank']],how='left',on='Plant ID')
            cap_data = cap_data.loc[:,['Plant ID','Commitment (Mt)','Cost rank']].copy(deep=True)
            cap_data = cap_data.sort_values(['Cost rank']).reset_index(drop=True)
            del source_cost_rank
            
            ccus_record = cap_data.copy(deep=True)
            ccus_record.insert(loc=ccus_record.shape[1],column='InstallMarker',value=0)
            ccus_record.loc[np.isin(ccus_record['Plant ID'],cap_record_last),'InstallMarker'] = 1
            for iyr in yearls2:
                ccus_record[iyr] = np.nan
                cap_yr = ccs_constain.loc[ccs_constain['Country']==ireg,str(iyr)]
                cap_all_yr = pd.merge(ccus_record,op_all.loc[:,['Plant ID',str(iyr)]],on='Plant ID',how='left')
                cap_all_yr.rename(columns={str(iyr):'Op_status'},inplace=True)
                cap_all_yr = cap_all_yr.sort_values(['InstallMarker','Cost rank'],ascending=[False,True]).reset_index(drop=True)
                
                cap_all_yr['Commitment_ref'] = cap_all_yr['Op_status']*cap_all_yr['Commitment (Mt)']*0.9
                cap_all_yr['Commitment_ref'] = cap_all_yr['Commitment_ref'].mask((cap_all_yr['Commitment_ref']==0)&(cap_all_yr['InstallMarker']==1),
                                                                                 cap_all_yr['InstallMarker']*cap_all_yr['Commitment (Mt)'])
                cap_all_yr.insert(loc=cap_all_yr.shape[1],column='ComitCumSum',value=cap_all_yr['Commitment_ref'].cumsum())
                
                if cap_all_yr['ComitCumSum'].max()<cap_yr.values[0]:
                    cap_all_yr['ComitCumSum'] = cap_all_yr['Commitment (Mt)'].cumsum()
                    filter1 = (cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)
                    plant_id = cap_all_yr.loc[filter1,'Plant ID']
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),iyr] = 1
                    ccus_record.loc[~np.isin(ccus_record['Plant ID'],plant_id),iyr] = 0
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),'InstallMarker'] = 1
                    
                    early_id = cap_all_yr.loc[(cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)&(cap_all_yr['Op_status']==0),['Plant ID']]
                    early_id.insert(loc=0,column='Year',value=iyr)
                    early_id_all = pd.concat([early_id_all,early_id],axis=0)
                    
                    op_all.loc[np.isin(op_all['Plant ID'],early_id_all),str(iyr):] = 1
                else:
                    filter1 = (cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)&(cap_all_yr['Op_status']>0)
                    plant_id = cap_all_yr.loc[filter1,'Plant ID']
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),iyr] = 1
                    ccus_record.loc[~np.isin(ccus_record['Plant ID'],plant_id),iyr] = 0
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),'InstallMarker'] = 1
        
        elif ior_sc in ['Age','Emis']:
            source_age_rank = pd.DataFrame()
            for isec in ['Power','Cement','IronAndSteel']:
                for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
                    if ior_sc == 'Age':
                        pha_order = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_AgeRank_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
                    elif ior_sc == 'Emis':
                        pha_order = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_EmisRank_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
                    source_age_rank = pd.concat([source_age_rank,pha_order],axis=0)
            source_age_rank.reset_index(drop=True,inplace=True)
            
            cap_data = pd.merge(cap_data,source_age_rank.loc[:,['Plant ID','Order']],how='left',on='Plant ID')
            cap_data = cap_data.loc[:,['Plant ID','Commitment (Mt)','Order']].copy(deep=True)
            cap_data = cap_data.sort_values(['Order'],ascending=False).reset_index(drop=True)
            del source_age_rank
            
            ccus_record = cap_data.copy(deep=True)
            ccus_record.insert(loc=ccus_record.shape[1],column='InstallMarker',value=0)
            ccus_record.loc[np.isin(ccus_record['Plant ID'],cap_record_last),'InstallMarker'] = 1
            for iyr in yearls2:
                ccus_record[iyr] = np.nan
                cap_yr = ccs_constain.loc[ccs_constain['Country']==ireg,str(iyr)]
                cap_all_yr = pd.merge(ccus_record,op_all.loc[:,['Plant ID',str(iyr)]],on='Plant ID',how='left')
                cap_all_yr.rename(columns={str(iyr):'Op_status'},inplace=True)
                cap_all_yr = cap_all_yr.sort_values(['InstallMarker','Order'],ascending=[False,False]).reset_index(drop=True)
                
                cap_all_yr['Commitment_ref'] = cap_all_yr['Op_status']*cap_all_yr['Commitment (Mt)']*0.9
                cap_all_yr['Commitment_ref'] = cap_all_yr['Commitment_ref'].mask((cap_all_yr['Commitment_ref']==0)&(cap_all_yr['InstallMarker']==1),
                                                                                 cap_all_yr['InstallMarker']*cap_all_yr['Commitment (Mt)'])
                cap_all_yr.insert(loc=cap_all_yr.shape[1],column='ComitCumSum',value=cap_all_yr['Commitment_ref'].cumsum())
                
                if cap_all_yr['ComitCumSum'].max()<cap_yr.values[0]:
                    cap_all_yr['ComitCumSum'] = cap_all_yr['Commitment (Mt)'].cumsum()
                    filter1 = (cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)
                    plant_id = cap_all_yr.loc[filter1,'Plant ID']
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),iyr] = 1
                    ccus_record.loc[~np.isin(ccus_record['Plant ID'],plant_id),iyr] = 0
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),'InstallMarker'] = 1
                    
                    early_id = cap_all_yr.loc[(cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)&(cap_all_yr['Op_status']==0),['Plant ID']]
                    early_id.insert(loc=0,column='Year',value=iyr)
                    early_id_all = pd.concat([early_id_all,early_id],axis=0)
                    
                    op_all.loc[np.isin(op_all['Plant ID'],early_id_all),str(iyr):] = 1
                    
                else:
                    filter1 = (cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)&(cap_all_yr['Op_status']>0)
                    plant_id = cap_all_yr.loc[filter1,'Plant ID']
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),iyr] = 1
                    ccus_record.loc[~np.isin(ccus_record['Plant ID'],plant_id),iyr] = 0
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),'InstallMarker'] = 1
    
    elif yr_beg != startyr:
        cap_data = pd.read_csv(SSM_dir+'/capture_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
        
        if ior_sc == 'Cost':
            cap_rec_last = pd.read_csv(CCSInstall_dir+ireg+'_CCSInstallPP_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv')
            cap_rec_last = cap_rec_last.reindex(columns=['Plant ID','Commitment (Mt)','Cost rank','InstallMarker']+yearls2.astype(str).tolist(),
                                                fill_value=1)
            cap_rec_last = cap_rec_last.loc[np.isin(cap_rec_last['Plant ID'],cap_data['Plant ID'])==0,:].sort_values(['Cost rank'],ascending=True).reset_index(drop=True)
            cap_rec_last['CommCumSum'] = cap_rec_last['Commitment (Mt)'].cumsum()
            
            for iyr in yearls2[1:]:
                nonret_cap = (cap_rec_last['Commitment (Mt)'].sum()/10*(iyr-yearls2[0]))
                phase_ID = cap_rec_last.loc[(cap_rec_last['CommCumSum']<=nonret_cap*(1+1/10000)),'Plant ID']
                cap_rec_last.loc[np.isin(cap_rec_last['Plant ID'],phase_ID),yearls2[int(iyr-yearls2[0]):].astype(str)] = 0

            df_commit_all = pd.read_csv(CCSInstall_dir+'/CommitmentAll_'+ireg+'_AllPP_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
            cap_rec_last['Commitment (Mt)'] = 0
            
            cap_comit = cap_rec_last.copy(deep=True)
            cap_comit.loc[:,yearls2.astype(str)] = cap_comit.loc[:,yearls2.astype(str)].mul(cap_comit['Commitment (Mt)'].values*0.9,axis=0)
            ccs_constain.loc[:,yearls2.astype(str)] = ccs_constain.loc[:,yearls2.astype(str)]-cap_comit.loc[:,yearls2.astype(str)].sum()
            
            cap_rec_last.to_csv(CCSInstall_dir+ireg+'_LastPeriodCCUSLeft_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
            
            source_cost_rank = pd.read_csv(NetworkStatus_dir+'/4_CostAndFlow/2_Cost_rank_'+ireg+'.csv')
            
            cap_data = pd.merge(cap_data,source_cost_rank.loc[:,['Plant ID','Cost rank']],how='left',on='Plant ID')
            cap_data = cap_data.loc[:,['Plant ID','Commitment (Mt)','Cost rank']].copy(deep=True)
            cap_data = cap_data.sort_values(['Cost rank']).reset_index(drop=True)
            del source_cost_rank
            
            ccus_record = cap_data.copy(deep=True)
            ccus_record.insert(loc=ccus_record.shape[1],column='InstallMarker',value=0)
            ccus_record.loc[np.isin(ccus_record['Plant ID'],cap_record_last),'InstallMarker'] = 1
            for iyr in yearls2:
                ccus_record[iyr] = np.nan
                cap_yr = ccs_constain.loc[ccs_constain['Country']==ireg,str(iyr)]
                cap_all_yr = pd.merge(ccus_record,op_all.loc[:,['Plant ID',str(iyr)]],on='Plant ID',how='left')
                cap_all_yr.rename(columns={str(iyr):'Op_status'},inplace=True)
                cap_all_yr = cap_all_yr.sort_values(['InstallMarker','Cost rank'],ascending=[False,True]).reset_index(drop=True)
                
                cap_all_yr['Commitment_ref'] = cap_all_yr['Op_status']*cap_all_yr['Commitment (Mt)']*0.9
                cap_all_yr['Commitment_ref'] = cap_all_yr['Commitment_ref'].mask((cap_all_yr['Commitment_ref']==0)&(cap_all_yr['InstallMarker']==1),
                                                                                 cap_all_yr['InstallMarker']*cap_all_yr['Commitment (Mt)'])
                cap_all_yr.insert(loc=cap_all_yr.shape[1],column='ComitCumSum',value=cap_all_yr['Commitment_ref'].cumsum())

                if cap_all_yr['ComitCumSum'].max()<cap_yr.values[0]:
                    cap_all_yr['ComitCumSum'] = cap_all_yr['Commitment (Mt)'].cumsum()
                    filter1 = (cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)
                    plant_id = cap_all_yr.loc[filter1,'Plant ID']
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),iyr] = 1
                    ccus_record.loc[~np.isin(ccus_record['Plant ID'],plant_id),iyr] = 0
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),'InstallMarker'] = 1
                    
                    early_id = cap_all_yr.loc[(cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)&(cap_all_yr['Op_status']==0),['Plant ID']]
                    early_id.insert(loc=0,column='Year',value=iyr)
                    early_id_all = pd.concat([early_id_all,early_id],axis=0)
                    
                    op_all.loc[np.isin(op_all['Plant ID'],early_id_all),str(iyr):] = 1
                    
                else:
                    filter1 = (cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)&(cap_all_yr['Op_status']>0)
                    plant_id = cap_all_yr.loc[filter1,'Plant ID']
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),iyr] = 1
                    ccus_record.loc[~np.isin(ccus_record['Plant ID'],plant_id),iyr] = 0
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),'InstallMarker'] = 1
        
        elif ior_sc in ['Age','Emis']:
            cap_rec_last = pd.read_csv(CCSInstall_dir+ireg+'_CCSInstallPP_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv')
            cap_rec_last = cap_rec_last.reindex(columns=['Plant ID','Commitment (Mt)','Order','InstallMarker']+yearls2.astype(str).tolist(),
                                                fill_value=1)
            cap_rec_last = cap_rec_last.loc[np.isin(cap_rec_last['Plant ID'],cap_data['Plant ID'])==0,:].sort_values(['Order'],ascending=True).reset_index(drop=True)
            cap_rec_last['CommCumSum'] = cap_rec_last['Commitment (Mt)'].cumsum()
            
            for iyr in yearls2[1:]:
                nonret_cap = (cap_rec_last['Commitment (Mt)'].sum()/10*(iyr-yearls2[0]))
                phase_ID = cap_rec_last.loc[(cap_rec_last['CommCumSum']<=nonret_cap*(1+1/10000)),'Plant ID']
                cap_rec_last.loc[np.isin(cap_rec_last['Plant ID'],phase_ID),yearls2[int(iyr-yearls2[0]):].astype(str)] = 0

            df_commit_all = pd.read_csv(CCSInstall_dir+'/CommitmentAll_'+ireg+'_AllPP_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
            cap_rec_last['Commitment (Mt)'] = cap_rec_last['Plant ID'].replace(df_commit_all['Plant ID'].values,df_commit_all['Commitment (Mt)'])
            cap_rec_last['Commitment (Mt)'] = 0
            
            cap_comit = cap_rec_last.copy(deep=True)
            cap_comit.loc[:,yearls2.astype(str)] = cap_comit.loc[:,yearls2.astype(str)].mul(cap_comit['Commitment (Mt)'].values*0.9,axis=0)
            ccs_constain.loc[:,yearls2.astype(str)] = ccs_constain.loc[:,yearls2.astype(str)]-cap_comit.loc[:,yearls2.astype(str)].sum()
            
            cap_rec_last.to_csv(CCSInstall_dir+ireg+'_LastPeriodCCUSLeft_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
            
            source_age_rank = pd.DataFrame()
            for isec in ['Power','Cement','IronAndSteel']:
                for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
                    if ior_sc == 'Age':
                        pha_order = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_AgeRank_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
                    elif ior_sc == 'Emis':
                        pha_order = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_EmisRank_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
                    source_age_rank = pd.concat([source_age_rank,pha_order],axis=0)
            source_age_rank.reset_index(drop=True,inplace=True)
            
            cap_data = pd.merge(cap_data,source_age_rank.loc[:,['Plant ID','Order']],how='left',on='Plant ID')
            cap_data = cap_data.loc[:,['Plant ID','Commitment (Mt)','Order']].copy(deep=True)
            cap_data = cap_data.sort_values(['Order'],ascending=False).reset_index(drop=True)
            del source_age_rank
            
            ccus_record = cap_data.copy(deep=True)
            ccus_record.insert(loc=ccus_record.shape[1],column='InstallMarker',value=0)
            ccus_record.loc[np.isin(ccus_record['Plant ID'],cap_record_last),'InstallMarker'] = 1
            for iyr in yearls2:
                ccus_record[iyr] = np.nan
                cap_yr = ccs_constain.loc[ccs_constain['Country']==ireg,str(iyr)]
                cap_all_yr = pd.merge(ccus_record,op_all.loc[:,['Plant ID',str(iyr)]],on='Plant ID',how='left')
                cap_all_yr.rename(columns={str(iyr):'Op_status'},inplace=True)
                cap_all_yr = cap_all_yr.sort_values(['InstallMarker','Order'],ascending=[False,False]).reset_index(drop=True)

                cap_all_yr['Commitment_ref'] = cap_all_yr['Op_status']*cap_all_yr['Commitment (Mt)']*0.9
                cap_all_yr['Commitment_ref'] = cap_all_yr['Commitment_ref'].mask((cap_all_yr['Commitment_ref']==0)&(cap_all_yr['InstallMarker']==1),
                                                                                 cap_all_yr['InstallMarker']*cap_all_yr['Commitment (Mt)'])
                cap_all_yr.insert(loc=cap_all_yr.shape[1],column='ComitCumSum',value=cap_all_yr['Commitment_ref'].cumsum())

                if cap_all_yr['ComitCumSum'].max()<cap_yr.values[0]:
                    cap_all_yr['ComitCumSum'] = cap_all_yr['Commitment (Mt)'].cumsum()
                    filter1 = (cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)
                    plant_id = cap_all_yr.loc[filter1,'Plant ID']
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),iyr] = 1
                    ccus_record.loc[~np.isin(ccus_record['Plant ID'],plant_id),iyr] = 0
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),'InstallMarker'] = 1
                    
                    early_id = cap_all_yr.loc[(cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)&(cap_all_yr['Op_status']==0),['Plant ID']]
                    early_id.insert(loc=0,column='Year',value=iyr)
                    early_id_all = pd.concat([early_id_all,early_id],axis=0)
                    
                    op_all.loc[np.isin(op_all['Plant ID'],early_id_all),str(iyr):] = 1
                    
                else:
                    filter1 = (cap_all_yr['ComitCumSum']<=cap_yr.values[0]*1.01)&(cap_all_yr['Op_status']>0)
                    plant_id = cap_all_yr.loc[filter1,'Plant ID']
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),iyr] = 1
                    ccus_record.loc[~np.isin(ccus_record['Plant ID'],plant_id),iyr] = 0
                    ccus_record.loc[np.isin(ccus_record['Plant ID'],plant_id),'InstallMarker'] = 1
                
                if iyr == yearls2[yearls2.shape[0]-1]:
                    ccus_record.loc[:,iyr] = 1
    
    early_id_all.to_csv(CCSInstall_dir+'EarlyOperatingUnit_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None,encoding='utf-8-sig')
    
    return ccus_record

def GID_emis(ccus_table,
            NewFuelEmis_dir,CCSInstall_dir,
            ireg,yr_beg,yr_end,yearls2):
    
    df = pd.DataFrame()
    for isec in pp_run:
        for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
            df_ = pd.read_csv(NewFuelEmis_dir+isec+'_'+ifa+'_'+ireg+'_AllPP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',encoding='utf-8-sig')
            df_.rename(columns={'Fake_Plant ID':'Plant ID','Plant ID':'Location_Plant ID'},inplace=True)
            df = pd.concat([df,df_],axis=0)   
    del df_
    df = df.reset_index(drop=True)

    df['CO2 Eta (%)'] = 0

    if yr_beg == startyr:
        for iyr in yearls2:
            plantid = ccus_table.loc[ccus_table[iyr]==1,'Plant ID']
            df.loc[np.isin(df['Plant ID'],plantid)&(df['Year']==iyr),'CO2 Eta (%)'] = 90

        df['CO2 Emissions'] = df['CO2 Emissions']*(100-df['CO2 Eta (%)'])/100

    else:
        GID_all = pd.read_csv(CCSInstall_dir+ireg+'_GIDAll_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv',encoding='utf-8-sig')
        GID_all = GID_all.loc[(GID_all['Year']==yr_beg)&(GID_all['CO2 Eta (%)']==90),'Plant ID']
        df.loc[np.isin(df['Plant ID'],GID_all),'CO2 Emissions'] = df.loc[np.isin(df['Plant ID'],GID_all),'CO2 Emissions']*10

        ccus_table2 = pd.read_csv(CCSInstall_dir+ireg+'_LastPeriodCCUSLeft_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
        for iyr in yearls2:
            plantid = ccus_table.loc[ccus_table[iyr]==1,'Plant ID']
            plantid2 = ccus_table2.loc[ccus_table2[str(iyr)]==1,'Plant ID']
            df.loc[((np.isin(df['Plant ID'],plantid))|(np.isin(df['Plant ID'],plantid2)))&(df['Year']==iyr),'CO2 Eta (%)'] = 90
        
        df['CO2 Emissions'] = df['CO2 Emissions']*(100-df['CO2 Eta (%)'])/100

    early_id_all = pd.read_csv(CCSInstall_dir+'EarlyOperatingUnit_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',encoding='utf-8-sig')
    if early_id_all.shape[0]>0:
        early_ref = df.loc[np.isin(df['Plant ID'],early_id_all['Plant ID']),:]
        early_ref = early_ref.sort_values(['Year'],ascending=True).reset_index(drop=True)
        early_ref = early_ref.drop_duplicates(['Plant ID'])

        early_add_set = pd.DataFrame(columns=early_ref.columns)
        for iyr in early_id_all['Year'].drop_duplicates():
            this_id = early_id_all.loc[early_id_all['Year']==iyr,'Plant ID']
            for iid in this_id:
                add_yr_to = early_ref.loc[early_ref['Plant ID']==iid,'Start Year'].values[0]
                early_add = early_ref.loc[early_ref['Plant ID']==iid,:]
                for iiyr in range(iyr,int(add_yr_to)):
                    early_add.loc[early_add['Plant ID']==iid,'Year'] = iiyr
                    early_add_set = pd.concat([early_add_set,early_add],axis=0)

        early_info = early_add_set.loc[:,['Year','Sector','Facility Type','Production']].groupby(['Year','Sector','Facility Type'],as_index=False).sum()
        ori_info = df.loc[:,['Year','Sector','Facility Type','Production']].groupby(['Year','Sector','Facility Type'],as_index=False).sum()
        ori_info.rename(columns={'Production':'ProductionAll'},inplace=True)
        
        scale_factor = pd.merge(early_info,ori_info,on=['Year','Sector','Facility Type'],how='left')
        scale_factor['SF'] = (scale_factor['ProductionAll']-scale_factor['Production'])/scale_factor['ProductionAll']
        scale_factor = pd.merge(df.loc[:,['Year','Sector','Facility Type']],scale_factor,on=['Year','Sector','Facility Type'],how='left')
        scale_factor = scale_factor.fillna(1)
        
        df.loc[:,['Production', 'Activity rates', 'CO2 Emissions']] = df.loc[:,['Production', 'Activity rates', 'CO2 Emissions']].mul(scale_factor['SF'].values,axis=0)   
        
        df = pd.concat([df,early_add_set],axis=0)
        df.reset_index(drop=True,inplace=True)
        
        EarlyYearSet = early_add_set.loc[:,['Plant ID','Year']].groupby(['Plant ID'],as_index=False).min()
        EarlyYearSet.rename(columns={'Year':'Start Year'},inplace=True)
        EarlyYearSet = pd.merge(EarlyYearSet,df['Plant ID'],on='Plant ID',how='right')
        
        df['Start Year'] = df['Start Year'].mask(pd.isnull(EarlyYearSet['Start Year'])==0,EarlyYearSet['Start Year'])
    else:
        pass
    
    df_test = df.loc[:,['Country','Year','Sector','Facility Type','Production','Activity rates','CO2 Emissions']].copy(deep=True)
    df_test = df_test.groupby(['Country','Year','Sector','Facility Type'],as_index=False).sum()
    
    return df,df_test

#%%
def CCS_main(ieng_sc, iend_sc, ior_sc, ireg,
             Turnover_dir, CCSInstall_dir, NewProdDistr_dir, NewFuelEmis_dir, NetworkStatus_dir, SSM_dir,
             yr_beg, yr_end):
    
    yearls2 = pd.Series(np.linspace(yr_beg, yr_end, int((yr_end - yr_beg) / gap) + 1)).astype(int)
    
    ccs_constain, cap_record_last = constrain_get(
        ieng_sc=ieng_sc, ireg=ireg,
        Turnover_dir=Turnover_dir, CCSInstall_dir=CCSInstall_dir,
        SSM_dir=SSM_dir, NewFuelEmis_dir=NewFuelEmis_dir,
        yr_beg=yr_beg, yr_end=yr_end, yearls2=yearls2
    )
    
    op_all = get_op(
        Turnover_dir=Turnover_dir, NewProdDistr_dir=NewProdDistr_dir, CCSInstall_dir=CCSInstall_dir,
        ireg=ireg, yr_beg=yr_beg, yr_end=yr_end
    )
    
    ccus_table = CCS_install(
        ccs_constain=ccs_constain, op_all=op_all, cap_record_last=cap_record_last,
        SSM_dir=SSM_dir, NetworkStatus_dir=NetworkStatus_dir,
        Turnover_dir=Turnover_dir, CCSInstall_dir=CCSInstall_dir,
        ior_sc=ior_sc, ireg=ireg, yr_beg=yr_beg, yr_end=yr_end, yearls2=yearls2
    )
    
    ccus_table.to_csv(CCSInstall_dir + ireg + '_CCSInstallPP_' + str(yr_beg) + '_' + str(yr_end) + '.csv', index=None)
    
    GID_df, GID_summary = GID_emis(
        ccus_table=ccus_table,
        NewFuelEmis_dir=NewFuelEmis_dir, CCSInstall_dir=CCSInstall_dir,
        ireg=ireg, yr_beg=yr_beg, yr_end=yr_end, yearls2=yearls2
    )
    
    GID_df.to_csv(CCSInstall_dir + ireg + '_GIDAll_' + str(yr_beg) + '_' + str(yr_end) + '.csv', index=None, encoding='utf-8-sig')
    GID_summary.to_csv(CCSInstall_dir + ireg + '_GIDSummary_' + str(yr_beg) + '_' + str(yr_end) + '.csv', index=None)
    
    if yr_beg == startyr:
        GID_df.to_csv(CCSInstall_dir + ireg + '_GIDAll_all.csv', index=None, encoding='utf-8-sig')
    else:
        GID_all = pd.read_csv(CCSInstall_dir + ireg + '_GIDAll_all.csv', encoding='utf-8-sig')
        GID_df = GID_df.loc[GID_df['Year'] != yr_beg, :]
        GID_all = pd.concat([GID_all, GID_df], axis=0).reset_index(drop=True)
        GID_all.to_csv(CCSInstall_dir + ireg + '_GIDAll_all.csv', index=None, encoding='utf-8-sig')
    
    return


#%%
if __name__ == '__main__':
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Age'
    ireg='India'
    
    yr_beg=2020
    yr_end=2030
    
    Turnover_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/0_Turnover/'
    CCSInstall_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/6_CCSInstall/'
    NewProdDistr_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/1_NewBuilt/'
    NetworkStatus_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/5_Network_'+str(yr_end)+'/'
    SSM_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/4_SSM/'
    NewFuelEmis_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/3_NewFuelEmis/'
    
    mkdir(CCSInstall_dir)
    CCS_main(ieng_sc,iend_sc,ior_sc,ireg,
             Turnover_dir,CCSInstall_dir,NewProdDistr_dir,NewFuelEmis_dir,NetworkStatus_dir,SSM_dir,
             yr_beg,yr_end)