# -*- coding: utf-8 -*-
"""
Created on Sun Apr  9 10:16:58 2023

@author: 92978

将新建产量反填到设备
"""

#%%
import pandas as pd
import numpy as np
#import sys
from S1_Global_ENV import *

#%%
def Newprod2pp(new_prod,old_et,
               Turnover_dir,CCSInstall_dir,
                isec,ifa,ireg,
                yr_beg,yr_end,yearls2):

    if yr_beg == startyr:
        op_status = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
        op_status.drop(['Order'],axis=1,inplace=True)
    else:
        op_status = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OperatingStatus_all.csv')
    
    if new_build_style == 'orderly':
        new_order = pd.read_csv('../../2_GetPPHarmonized/output/6_CCSRank_Age/'+isec+'_'+ifa+'_'+ireg+'.csv')
        new_order = new_order.sort_values(['Plant ID']).reset_index(drop=True)
        new_order.rename(columns={'Age rank':'Rank'},inplace=True)
        
        pha_dict = pd.read_excel('../input/dict/DictOfPhaseout.xlsx',sheet_name='Max_Load_Life')
        cap_max = pha_dict.loc[pha_dict['Sector']==isec,'Newbuilt_Capacity'].values[0]
        cf = pha_dict.loc[pha_dict['Sector']==isec,'CFMax'].values[0]
        cf_method = pha_dict.loc[pha_dict['Sector']==isec,'CFMethod'].values[0]
        
        if cf_method == 'Decrease':
            if isec == 'Power':
                new_prod_max = np.tile([cap_max*2*10**3*0.5*(1-cf)**(iyr-startyr) for iyr in yearls2],[new_prod.shape[0],1])
            else:
                new_prod_max = np.tile([cap_max*2*10**3*0.85*(1-cf)**(iyr-startyr) for iyr in yearls2],[new_prod.shape[0],1])
        
        else:
            new_prod_max = np.tile([cap_max*2*10**3*0.85 for iyr in yearls2],[new_prod.shape[0],1])
        
        op_status = op_status.sort_values(['Plant ID']).reset_index(drop=True)
        op_status = pd.merge(new_order.loc[:,['Plant ID','Rank']],op_status,on='Plant ID',how='left')
        
        op_status = op_status.sort_values(['Rank']).reset_index(drop=True)
        op_status['Fake_Plant ID'] = isec+'_'+ifa+'_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+op_status.index.astype(str)
        #########################################################################
            
        coun_build = new_prod.copy(deep=True)
        coun_build.loc[:,yearls2.astype(str)] = coun_build.loc[:,yearls2.astype(str)].mask(coun_build==0,-1*new_prod_max)
        coun_build.loc[:,yearls2.astype(str)] = (coun_build.loc[:,yearls2.astype(str)]/new_prod_max).astype(int)+1
        
        record_build = pd.DataFrame(data=None,columns=['Fake_Plant ID','Plant ID','Country']+yearls2.tolist())
        
        if yr_beg != startyr:
            GID_all = pd.read_csv(CCSInstall_dir+ireg+'_GIDAll_all.csv',encoding='utf-8-sig')
            GID_havebuilt = GID_all.loc[GID_all['Plant ID']!=GID_all['Location_Plant ID'],'Location_Plant ID'].drop_duplicates()
            del GID_all
        else:
            GID_havebuilt = []
        
        for iyr in range(yearls2.shape[0]):
            a = op_status.loc[:,['Fake_Plant ID','Plant ID','Country','Rank',str(yearls2[iyr])]]
            a.columns = ['Fake_Plant ID','Plant ID','Country','Rank','Op_status']
            b = coun_build.loc[:,['Country',str(yearls2[iyr])]]
            b.columns = ['Country','Construction']
                 
            info_cons = pd.merge(a,b,on='Country',how='left')
            
            info_cons.loc[np.isin(info_cons['Plant ID'],record_build['Plant ID']),'Op_status'] = -1
            info_cons.loc[np.isin(info_cons['Plant ID'],GID_havebuilt),'Op_status'] = 2
            
            info_cons = info_cons.sort_values(['Country','Op_status','Rank'],ascending=[True,True,True]).reset_index(drop=True)
            # info_cons = info_cons.sort_values(['Country','Rank'],ascending=[True,False]).reset_index(drop=True)
            info_cons['CumCount'] = info_cons.groupby(['Country']).cumcount()+1
            
            build_filter = (info_cons['Construction']>=info_cons['CumCount'])
            build_info = info_cons.loc[build_filter,:]
            
            input_df = build_info.loc[np.isin(build_info['Fake_Plant ID'],record_build['Fake_Plant ID'])==0,['Fake_Plant ID','Plant ID','Country']]
            #input_df.insert(loc=input_df.shape[1],column='Build_yr',value=iyr)
            input_df = input_df.reindex(columns=record_build.columns,fill_value=np.nan)
            
            record_build = pd.concat([record_build,input_df],axis=0)
            record_build.loc[np.isin(record_build['Fake_Plant ID'],input_df['Fake_Plant ID']),yearls2[:iyr]] = 0
            record_build.loc[np.isin(record_build['Fake_Plant ID'],input_df['Fake_Plant ID']),yearls2[iyr:]] = 1
        
        record_build = record_build.sort_values(['Fake_Plant ID']).reset_index(drop=True)
        record_build = pd.merge(new_order.loc[:,['Plant ID','Rank']],record_build,how='right',on='Plant ID')
        record_build = record_build.sort_values(['Country','Rank'],ascending=[True,False]).reset_index(drop=True)
        
        dis_reference = record_build.copy(deep=True)
        dis_reference.loc[:,yearls2] = 0
        for iyr in yearls2:
            record_yr = record_build.loc[record_build[iyr]>0,['Fake_Plant ID','Plant ID','Country',iyr]]
            if record_yr.shape[0] == 0:
                continue
            record_yr = record_yr.reset_index(drop=True)
            record_yr[iyr] = record_yr[iyr].astype(float)
            record_yr['Count'] = record_yr.loc[:,['Country',iyr]].groupby(['Country']).cumsum()
            # record_build.loc[:,yearls] = record_build.loc[:,yearls].astype(float)
            # # rec_coun = record_build.groupby(['Country'],as_index=False).sum().loc[:,['Country']+yearls.tolist()]
            record_yr = pd.merge(record_yr,coun_build.loc[:,['Country',str(iyr)]],on='Country',how='left')
            
            record_yr.columns = ['Fake_Plant ID', 'Plant ID', 'Country', 'BuildNum', 'Count', 'NeedNum']
            filtered_dis = record_yr.loc[(record_yr['Count']<=record_yr['NeedNum']),'Fake_Plant ID']
            
            dis_reference.loc[np.isin(dis_reference['Fake_Plant ID'],filtered_dis),iyr] = 1
        
        # dis_reference.loc[:,yearls.astype(str).tolist()] = dis_reference.loc[:,yearls.astype(str).tolist()].mask(rec_coun==0,-1)
        rec_coun = dis_reference.loc[:,['Country']+yearls2.tolist()].groupby(['Country'],as_index=False).sum()
        rec_coun = pd.merge(dis_reference.loc[:,['Country']],rec_coun,on='Country',how='left')
        rec_coun.loc[:,yearls] = rec_coun.loc[:,yearls2].mask(rec_coun.loc[:,yearls2]==0,-1)
        
        ratio_all = rec_coun.copy(deep=True)
        ratio = dis_reference.loc[:,yearls2].values/rec_coun.loc[:,yearls2].values
        ratio_all.loc[:,yearls2] = ratio
        ratio_all.loc[:,yearls2] = ratio_all.loc[:,yearls2].mask(ratio_all.loc[:,yearls2]<0,0)
        
        mer_prod = pd.merge(record_build.loc[:,['Fake_Plant ID','Plant ID','Country','Rank']],new_prod,on='Country',how='left')
        mer_prod = mer_prod.sort_values(['Country','Rank'],ascending=[True,False]).reset_index(drop=True)
        mer_prod.loc[:,yearls2.astype(str)] = mer_prod.loc[:,yearls2.astype(str)].values * ratio_all.loc[:,yearls2].values
        
        start_yr_ls = record_build[yearls2].mul(yearls2.values,axis=1)
        start_yr_ls = start_yr_ls.mask(start_yr_ls==0,9999).min(axis=1)
        record_build['Start Year'] = start_yr_ls
        
        mer_prod = pd.merge(record_build.loc[:,['Fake_Plant ID','Start Year']],mer_prod,
                            on='Fake_Plant ID',how='right')
        
    return mer_prod,dis_reference#mer_ep,mer_ec,


#%%
def NewProdDistr_main(ieng_sc,iend_sc,ior_sc,
                      isec,ifa,ireg,
                      Turnover_dir,NewProdDistr_dir,CCSInstall_dir,
                      yr_beg,yr_end):
    
    yearls2 = pd.Series(np.linspace(yr_beg,yr_end,int((yr_end-yr_beg)/gap)+1)).astype(int)
    
    new_prod_tot = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_NewUnitProduction_Coun_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    old_prod_pp = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_OldUnitProduction_PP_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    new_prod_pp,new_op = Newprod2pp(new_prod=new_prod_tot,old_et=old_prod_pp,
                                    Turnover_dir=Turnover_dir,CCSInstall_dir=CCSInstall_dir,
                                    isec=isec,ifa=ifa,ireg=ireg,
                                    yr_beg=yr_beg,yr_end=yr_end,yearls2=yearls2)
    
    new_prod_pp.to_csv(NewProdDistr_dir+isec+'_'+ifa+'_'+ireg+'_NewProduction_PP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    new_op.to_csv(NewProdDistr_dir+isec+'_'+ifa+'_'+ireg+'_NewPP_OperatingStatus_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    return

#%%
if __name__ == '__main__':
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Cost'
    isec='Power'
    ifa='Coal'
    ireg='India'
    Turnover_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/0_Turnover/'
    NewProdDistr_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/1_NewBuilt/'
    CCSInstall_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/6_CCSInstall/'
    
    yr_beg=2030
    yr_end=2040
    
    mkdir(NewProdDistr_dir)
    NewProdDistr_main(ieng_sc,iend_sc,ior_sc,
                      isec,ifa,ireg,
                      Turnover_dir,NewProdDistr_dir,CCSInstall_dir,
                      yr_beg,yr_end)
