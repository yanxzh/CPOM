# -*- coding: utf-8 -*-
"""
Created on Thu Apr  4 16:35:42 2024

@author: 92978
"""

#%%
import numpy as np
import pandas as pd
import geopandas as gpd
import time
from gurobipy import *
import random
from S0_COST_ENV import *
from S1_Global_ENV import *

#%%
def get_sink(ireg,yr_beg,yr_end,SSM_dir):
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
    so_si.loc[:,['Longitude','Latitude','CO2','Capacity','DSA']] = \
        so_si.loc[:,['Longitude','Latitude','CO2','Capacity','DSA']].astype(float)
        
    so_si = so_si.rename(columns={'ID':'Plant ID'})
    
    if np.isin(yr_beg,[startyr,startyr+10]):
        si_data = so_si.loc[so_si['Type']=='Sink',:].reset_index(drop=True).copy(deep=True)
        si_data.loc[:,'CO2'] = si_data.loc[:,'CO2']*10**3
    else:
        si_data = pd.read_csv(SSM_dir+ireg+'_UpdateSink_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv',encoding='utf-8-sig')
        
    if yr_beg != startyr:
        sink_red = pd.read_csv(SSM_dir+ireg+'_StorageReduce_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv',encoding='utf-8-sig')
        sink_red.columns = ['Plant ID','Capture (Mt)']
        sink_red = pd.merge(si_data['Plant ID'],sink_red,on='Plant ID',how='left')
        sink_red = sink_red.fillna(0)
        
        si_data['CO2'] = si_data['CO2']-sink_red['Capture (Mt)'].values
        si_data = si_data.loc[si_data['CO2']>0,:].reset_index(drop=True)
        si_data.to_csv(SSM_dir+ireg+'_UpdateSink_'+str(yr_beg)+'_'+str(yr_end)+'.csv',encoding='utf-8-sig',index=None)
        
    return si_data,so_si

def get_source(ieng_sc,ior_sc,
               so_si,
               NewFuelEmis_dir,Turnover_dir,SSM_dir,
               ireg,yr_beg,yr_end):
    
    df = pd.DataFrame()
    constrain = 0
    
    for isec in pp_run:
        for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
            df_ = pd.read_csv(NewFuelEmis_dir+isec+'_'+ifa+'_'+ireg+'_AllPP_'+str(yr_beg)+'_'+str(yr_end)+'.csv',encoding='utf-8-sig')
            col = ['Fake_Plant ID','Plant ID','Country','Year','CO2 Emissions',
                   'Sector','Facility Type', 'Longitude', 'Latitude',
                   'Start Year','Age','Capacity','Capacity Unit','CO2 Eta (%)']
            df_ = df_.loc[(df_['Year']==yr_end),col].reset_index(drop=True)
            # df_ = df_.iloc[:100,:]
    
            df_.rename(columns={'Fake_Plant ID':'Plant ID','Plant ID':'Location_Plant ID'},inplace=True)
            
            pha_dict = pd.read_excel('../input/dict/DictOfPhaseout.xlsx',sheet_name='Max_Load_Life')
            max_life = pha_dict.loc[pha_dict['Sector']==isec,'LifeTime'].values[0]
            del pha_dict
            
            df_['Age'] = yr_end-df_['Start Year'].astype(float)
            
            df_['Commitment (Mt)'] = df_['CO2 Emissions']/((100-df_['CO2 Eta (%)'])/100)*(max_life-df_['Age'])/10**6
            
            df_ = df_.loc[df_['Commitment (Mt)']>0,:].reset_index(drop=True)
            # df_.drop(['CO2 Eta (%)'],axis=1,inplace=True)
            
            constrain = constrain + get_constrain(ieng_sc=ieng_sc,isec=isec,ifa=ifa,ireg=ireg,yr_end=yr_end,comit=df_['Commitment (Mt)'].sum())
            
            df_ = df_.loc[(np.isin(df_['Location_Plant ID'],so_si['Plant ID'])),:].reset_index(drop=True)

            df = pd.concat([df,df_],axis=0)

    pha_order = pd.DataFrame()
    for isec in pp_run:
        for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
            if yr_beg == startyr:
                pha_order_ = pd.read_csv('../../2_GetPPHarmonized/output/3_AgeRank/'+isec+'_'+ifa+'.csv')
                pha_order_ = pha_order_.loc[:,['Plant ID','Age']]
                # pha_order_.rename(columns={'Age rank':'Order'},inplace=True)
                # df_ = pd.merge(df_,pha_order,on='Plant ID',how='left')#
            else:
                pha_order_ = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_AgeRank_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv')
                pha_order_.drop(['Age'],axis=1,inplace=True)
                # df_ = pd.merge(df_,pha_order,on='Plant ID',how='left')#
            
            pha_order = pd.concat([pha_order,pha_order_],axis=0)
            
    if yr_beg == startyr:
        pha_order['Random_age'] = np.random.randint(0,high=pha_order.shape[0],
                                                        size=pha_order.shape[0],dtype='l')
        pha_order = pha_order.sort_values(['Age','Random_age'],ascending=[False,True]).reset_index(drop=True)
        pha_order.reset_index(drop=False,inplace=True)
        pha_order.rename(columns={'index':'Order'},inplace=True)
        pha_order.drop(['Random_age','Age'],axis=1,inplace=True)

    df = pd.merge(df,pha_order,on='Plant ID',how='left')#
                
    np.random.seed(2)
    change = df.loc[pd.isnull(df['Order']),['Plant ID','Age']].copy(deep=True)
    change['Random_age'] = np.random.randint(0,high=change.shape[0],
                                             size=change.shape[0],dtype='l')
    change = change.sort_values(['Age','Random_age'],ascending=[False,True]).reset_index(drop=True)
    change.reset_index(drop=False,inplace=True)
    change.rename(columns={'index':'Order'},inplace=True)
    change['Order'] = change['Order']+df['Order'].max()+1
    
    change = pd.merge(df.loc[pd.isnull(df['Order']),['Plant ID']],change,on='Plant ID',how='left')
    df.loc[pd.isnull(df['Order']),'Order'] = change['Order'].values
    
    for isec in ['Power','Cement','IronAndSteel']:
        for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
            pha_order = df.loc[(df['Sector']==isec)&(df['Facility Type']==ifa),['Plant ID','Age','Order']]
            pha_order.rename(columns={'Order':'Age rank'})
            pha_order.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_AgeRank_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    ########################################################################
    
    ########################################################################
    if ior_sc == 'Emis':
        if yr_beg == startyr:
            np.random.seed(2)
            change = df.loc[:,['Plant ID','CO2 Emissions']].copy(deep=True)
            change['Random_emis'] = np.random.randint(0,high=change.shape[0],
                                                     size=change.shape[0],dtype='l')
            change = change.sort_values(['CO2 Emissions','Random_emis'],ascending=[True,True]).reset_index(drop=True)
            change.reset_index(drop=False,inplace=True)
            change.rename(columns={'index':'Order'},inplace=True)
            
            change = pd.merge(df.drop(['Order'],axis=1),change.loc[:,['Plant ID','Order']],on='Plant ID',how='left')
            df['Order'] = change['Order'].values

        elif yr_beg != startyr:
            pha_order = pd.DataFrame()
            for isec in pp_run:
                for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
                    pha_order_ = pd.read_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_EmisRank_'+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv')
                    pha_order = pd.concat([pha_order,pha_order_],axis=0)
            
            df = pd.merge(df.drop(['Order'],axis=1),pha_order.drop(['CO2 Emissions'],axis=1),on='Plant ID',how='left')#
                        
            np.random.seed(2)
            change = df.loc[pd.isnull(df['Order']),['Plant ID','CO2 Emissions']].copy(deep=True)
            change['Random_emis'] = np.random.randint(0,high=change.shape[0],
                                                     size=change.shape[0],dtype='l')
            change = change.sort_values(['CO2 Emissions','Random_emis'],ascending=[True,True]).reset_index(drop=True)
            change.reset_index(drop=False,inplace=True)
            change.rename(columns={'index':'Order'},inplace=True)
            change['Order'] = change['Order']+df['Order'].max()+1
            
            change = pd.merge(df.loc[pd.isnull(df['Order']),['Plant ID']],change,on='Plant ID',how='left')
            df.loc[pd.isnull(df['Order']),'Order'] = change['Order'].values
            
        for isec in ['Power','Cement','IronAndSteel']:
            for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
                pha_order = df.loc[(df['Sector']==isec)&(df['Facility Type']==ifa),['Plant ID','CO2 Emissions','Order']]
                
                pha_order.to_csv(Turnover_dir+isec+'_'+ifa+'_'+ireg+'_EmisRank_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    ########################################################################
    
    ################
    #############################
    if ior_sc in ['Age','Emis']:
        if yr_beg == startyr:
            df = df.sort_values(['Order'],ascending=False).reset_index(drop=True)
            df['CumComit'] = df['Commitment (Mt)'].cumsum()
            df = df.loc[df['CumComit']<=constrain*2,:].reset_index(drop=True)
            df.drop(['CumComit'],axis=1,inplace=True)
            
        else:
            #
            r_par_all = pd.read_csv(SSM_dir+'/capture_sec_'+ireg+'_marker.csv')
            r_par_all = r_par_all.loc[(r_par_all['Year']==yr_beg)&(r_par_all['CCUSInstall']==0),:]
            
            df = df.sort_values(['Order'],ascending=False).reset_index(drop=True)
            df['CumComit'] = df['Commitment (Mt)'].cumsum()
            df = df.loc[(df['CumComit']<=constrain*2)|(np.isin(df['Plant ID'],r_par_all['Plant ID'])),:].reset_index(drop=True)
            df.drop(['CumComit'],axis=1,inplace=True)
            
    df.drop(['Order'],axis=1,inplace=True)
    df.reset_index(drop=True,inplace=True)
        
    return df,constrain

#
def get_constrain(ieng_sc,isec,ifa,ireg,yr_end,comit):
    ccs_dem_coun = pd.read_csv('../../2_GetPPHarmonized/output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_CCS_RegionTrend.csv')
    dem_coun = pd.read_csv('../../2_GetPPHarmonized/output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_RegionTrend.csv')
    
    ccs_dem = ccs_dem_coun.loc[ccs_dem_coun['Region']==ireg,str(yr_end)]
    all_dem = dem_coun.loc[ccs_dem_coun['Region']==ireg,str(yr_end)]
    
    constrain = comit*ccs_dem/all_dem*0.9#Mt
    constrain = constrain.values[0]
    
    return constrain

#
def get_edge(so_data,si_data,SSM_dir,ireg,yr_beg,yr_end):
    print('Edge in')
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
    
    all_loc = pd.concat([so_data.loc[:,['Plant ID','Longitude','Latitude']],
                          si_data.loc[:,['Plant ID','Longitude','Latitude']]],axis=0)
    
    #
    pipline_net = pd.read_pickle('../../4_SSM/PreNet/PreNet_'+reg2+'/output/4_Network/Point2Point_'+reg1+'.pkl')
    pipline_net['Distances (km)'] = pipline_net['Distances (km)'].astype(float)
    pipline_net = pipline_net.loc[pipline_net['Distances (km)']<=max_distance,:]
    
    pipline_net = FakePoint_Renet(net=pipline_net,df=so_data,yr_beg=yr_beg,yr_end=yr_end)
    
    all_loc = all_loc.loc[(np.isin(all_loc['Plant ID'],pipline_net['Start'])|(np.isin(all_loc['Plant ID'],pipline_net['End']))),:]
    if yr_end != 2030:
        all_loc2 = pd.read_csv(SSM_dir+'/AllLoc4Net_'+ireg+str(yr_beg-10)+'_'+str(yr_end-10)+'.csv')
        all_loc = pd.concat([all_loc,all_loc2.loc[np.isin(all_loc2['Plant ID'],all_loc['Plant ID'])==0,['Plant ID','Longitude','Latitude']]],axis=0)
    all_loc.to_csv(SSM_dir+'/AllLoc4Net_'+ireg+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    all_loc = all_loc.reset_index(drop=True).reset_index(drop=False)
    
    begin = np.repeat(all_loc['Plant ID'].values,all_loc.shape[0],axis=0)
    end = np.tile(all_loc['Plant ID'].values,[1,all_loc.shape[0]])[0].T
    all_dis = pd.DataFrame([begin,end]).T
    all_dis.columns = ['Start_index','End_index']
    all_dis = pd.merge(all_dis,all_loc,left_on=['Start_index'],right_on='Plant ID')
    all_dis = all_dis.drop(['Start_index','index'],axis=1)
    all_dis.columns = ['End_index','Start','Lon_st','Lat_st']
    all_dis = pd.merge(all_dis,all_loc,left_on=['End_index'],right_on='Plant ID')
    all_dis = all_dis.drop(['End_index','index'],axis=1)
    all_dis.columns = ['Start','Lon_st','Lat_st','End','Lon_en','Lat_en']
    all_dis = all_dis.loc[all_dis['Start']!=all_dis['End'],:].reset_index(drop=True)
    

    all_dis2 = pd.merge(all_dis,pipline_net,on=['Start','End'],how='left')
    
    del pipline_net,all_dis,begin,end
    
    all_dis2['Distance'] = all_dis2['Distances (km)']
    all_dis2 = all_dis2.drop(['Distances (km)'],axis=1)
    # all_dis2 = all_dis2.loc[all_dis2['Distance']>0,:]
    all_dis2 = all_dis2.loc[pd.isnull(all_dis2['Distance'])==0,:]
    all_dis2 = all_dis2.reset_index(drop=True)
    
    return all_dis2,all_loc

#
def FakePoint_Renet(net,df,yr_beg,yr_end):
    net_ori = net.copy(deep=True)
    
    #
    filtered = df['Plant ID'].str.contains('0_20')
    df = df.loc[filtered,:].copy(deep=True)
    
    df_add = df.loc[:,['Location_Plant ID','Plant ID']]
    df_add['Distances (km)'] = 0.1
    df_add.columns = ['Start','End','Distances (km)']
    
    df_add1 = df_add.copy(deep=True)
    df_add1.columns = ['End','Start','Distances (km)']
    
    df_add = pd.concat([df_add,df_add1],axis=0)
    del df_add1
    
    #
    ###################
    net_add_st = net_ori.loc[(np.isin(net_ori['Start'],df['Location_Plant ID'])),:].copy(deep=True)
    net_add_st = pd.merge(df.loc[:,['Location_Plant ID','Plant ID']],
                          net_add_st.rename(columns={'Start':'Location_Plant ID'}),
                          on='Location_Plant ID')
    net_add_st.drop(['Location_Plant ID'],axis=1,inplace=True)
    net_add_st.rename(columns={'Plant ID':'Start'},inplace=True)
    ###################
    
    ###################
    net_add_en = net_ori.loc[(np.isin(net_ori['End'],df['Location_Plant ID'])),:].copy(deep=True)
    net_add_en = pd.merge(df.loc[:,['Location_Plant ID','Plant ID']],
                          net_add_en.rename(columns={'End':'Location_Plant ID'}),
                          on='Location_Plant ID')
    net_add_en.drop(['Location_Plant ID'],axis=1,inplace=True)
    net_add_en.rename(columns={'Plant ID':'End'},inplace=True)
    ###################
    
    ################
    net_add_bo = net_ori.loc[(np.isin(net_ori['Start'],df['Location_Plant ID']))&\
                             (np.isin(net_ori['End'],df['Location_Plant ID'])),:].copy(deep=True)
    net_add_bo = pd.merge(df.loc[:,['Location_Plant ID','Plant ID']],
                          net_add_bo.rename(columns={'Start':'Location_Plant ID'}),
                          on='Location_Plant ID')
    net_add_bo.drop(['Location_Plant ID'],axis=1,inplace=True)
    net_add_bo.rename(columns={'Plant ID':'Start'},inplace=True)
    
    net_add_bo = pd.merge(df.loc[:,['Location_Plant ID','Plant ID']],
                          net_add_bo.rename(columns={'End':'Location_Plant ID'}),
                          on='Location_Plant ID')
    net_add_bo.drop(['Location_Plant ID'],axis=1,inplace=True)
    net_add_bo.rename(columns={'Plant ID':'End'},inplace=True)
    ################
    
    net_ori = pd.concat([net_ori,net_add_st,net_add_en,net_add_bo],axis=0)
    net_ori.reset_index(drop=True,inplace=True)
    
    return net_ori

def SSM_drive(so_data,si_data,
              edge_distance,all_loc,
              ireg,yr_beg,yr_end,
              constrain,SSM_dir):
    
    a = time.time()
    
    # Create a Gurobi model
    m = Model()
    
    ####################################marker##########################
    if yr_beg == startyr:
        r_par = pd.DataFrame(data=1,index=so_data.index,columns=['CCUSInstall'])
        
        edge_distance.insert(value=1,loc=edge_distance.shape[1],column='PiplineInstall1')
        edge_distance.insert(value=1,loc=edge_distance.shape[1],column='PiplineInstall2')
        edge_distance.insert(value=1,loc=edge_distance.shape[1],column='PiplineInstall3')
        edge_distance.insert(value=1,loc=edge_distance.shape[1],column='PiplineInstall4')    
        
        i_par = pd.DataFrame(data=1,index=si_data.index,columns=['StorageConstruct'])
     
    else:
        edge_distance_all = pd.read_csv(SSM_dir+'/transport_sec_'+ireg+'_marker.csv')
        r_par_all = pd.read_csv(SSM_dir+'/capture_sec_'+ireg+'_marker.csv')
        i_par_all = pd.read_csv(SSM_dir+'/store_sec_'+ireg+'_marker.csv')
        
        #############################
        r_par = pd.DataFrame(data=1,index=so_data.index,columns=['CCUSInstall'])
        filtered_id = r_par_all.loc[r_par_all['CCUSInstall']==0,'Plant ID']
        r_par.loc[np.isin(so_data['Plant ID'],filtered_id),'CCUSInstall'] = 0
        
        edge_id_all = edge_distance_all['Start']+'_'+edge_distance_all['End']
        edge_id_this = edge_distance['Start']+'_'+edge_distance['End']
        
        edge_distance.insert(value=1,loc=edge_distance.shape[1],column='PiplineInstall1')
        filtered_id = edge_id_all[edge_distance_all['PiplineInstall1']==0]
        edge_distance.loc[np.isin(edge_id_this,filtered_id),'PiplineInstall1'] = 0
        
        edge_distance.insert(value=1,loc=edge_distance.shape[1],column='PiplineInstall2')
        filtered_id = edge_id_all[edge_distance_all['PiplineInstall2']==0]
        edge_distance.loc[np.isin(edge_id_this,filtered_id),'PiplineInstall2'] = 0
        
        edge_distance.insert(value=1,loc=edge_distance.shape[1],column='PiplineInstall3')
        filtered_id = edge_id_all[edge_distance_all['PiplineInstall3']==0]
        edge_distance.loc[np.isin(edge_id_this,filtered_id),'PiplineInstall3'] = 0
        
        edge_distance.insert(value=1,loc=edge_distance.shape[1],column='PiplineInstall4')    
        filtered_id = edge_id_all[edge_distance_all['PiplineInstall4']==0]
        edge_distance.loc[np.isin(edge_id_this,filtered_id),'PiplineInstall4'] = 0
        
        #
        i_par = pd.DataFrame(data=1,index=si_data.index,columns=['StorageConstruct'])
        filtered_id = i_par_all.loc[i_par_all['StorageConstruct']==0,'Plant ID']
        i_par.loc[np.isin(si_data['Plant ID'],filtered_id),'StorageConstruct'] = 0
        
        del edge_distance_all,r_par_all,i_par_all
    ##############################################################
    
    ##############################################
    cos_ret = pd.read_csv('../../2_GetPPHarmonized/output/PlantCaptureCost.csv')
    cos_ret = pd.merge(so_data.loc[:,['Plant ID','Sector','Facility Type']],cos_ret,on='Plant ID',how='left')
    # cos_ret = cos_ret['CaptureCost']
    
    cost_dict = pd.read_excel('../../2_GetPPHarmonized/input/Dict_cost/Dict_RetrofitCost.xlsx',sheet_name='Final')
    pha_dict = pd.read_excel('../input/dict/DictOfPhaseout.xlsx',sheet_name='Max_Load_Life')
    
    for isec in pp_run:
        for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
            unit_cap = pha_dict.loc[pha_dict['Sector']==isec,'Newbuilt_Capacity'].values[0]
            
            if isec == 'Power':
                unit_cost = cost_dict.loc[cost_dict['Facility Type']==ifa,'UnitCaptureCost'].values[0]
                cos_ret.loc[(pd.isnull(cos_ret['CaptureCost']))&(cos_ret['Facility Type']==ifa),'CaptureCost'] = unit_cap/8760*2*unit_cost
            elif isec == 'IronAndSteel':
                unit_cost = cost_dict.loc[cost_dict['Facility Type']=='BF','UnitCaptureCost'].values[0]
                cos_ret.loc[(pd.isnull(cos_ret['CaptureCost']))&(cos_ret['Facility Type']==ifa),'CaptureCost'] = unit_cap*2*unit_cost
            elif isec == 'Cement':
                unit_cost = cost_dict.loc[cost_dict['Facility Type']=='Dry with preheater and precalciner','UnitCaptureCost'].values[0]
                cos_ret.loc[(pd.isnull(cos_ret['CaptureCost']))&(cos_ret['Facility Type']==ifa),'CaptureCost'] = unit_cap*2*unit_cost
    
    cos_ret = cos_ret['CaptureCost']
    
    del cost_dict,unit_cost,pha_dict

    s_var = m.addVars(so_data.shape[0], vtype=GRB.BINARY, name='s_var')
    
    changing_rate = pd.read_excel('../input/dict/Dict_Capture.xlsx',sheet_name='ComitPrediction',usecols=['Year','Comitted capture (Mt)'])
    changing_rate_power = (changing_rate.loc[changing_rate['Year']==yr_end,'Comitted capture (Mt)'].values/changing_rate.loc[changing_rate['Year']==2021,'Comitted capture (Mt)'].values)[0]**np.log2((1-0.089))
    changing_rate_other = (changing_rate.loc[changing_rate['Year']==yr_end,'Comitted capture (Mt)'].values/changing_rate.loc[changing_rate['Year']==2021,'Comitted capture (Mt)'].values)[0]**np.log2((1-0.05))
    
    cos_ret[so_data['Sector']=='Power'] = cos_ret[so_data['Sector']=='Power']*changing_rate_power
    cos_ret[so_data['Sector']!='Power'] = cos_ret[so_data['Sector']!='Power']*changing_rate_other
    
    #
    Potential_rot = (cos_ret*r_par['CCUSInstall'].values)
    
    del changing_rate_power,changing_rate_other

    changing_rate_cap = (changing_rate.loc[changing_rate['Year']==yr_end,'Comitted capture (Mt)'].values/changing_rate.loc[changing_rate['Year']==2021,'Comitted capture (Mt)'].values)[0]**np.log2((1-0.125))
    cos_cap2 = cos_cap*changing_rate_cap
    Potential_cap = cos_cap2*so_data['Commitment (Mt)'].values*0.9#(cos_cap-ben_cp)
    
    del changing_rate_cap
    

    filter1 = np.isin(so_data['Plant ID'],all_loc['Plant ID'])
    m.addConstr(sum(s_var.select(np.where(~filter1)[0]))==0,name='So_Zero')
    del filter1

    if yr_beg != startyr:
        filter1 = (r_par['CCUSInstall']==0)
        for iloc in np.where(filter1)[0]:
            m.addConstr(s_var[iloc]==1,name='So_cap'+str(iloc))
        try:
            del filter1,iloc
        except:
            pass


    #%%
    i_var = m.addVars(si_data.shape[0], vtype=GRB.BINARY, name='i_var')
    d_var = m.addVars(si_data.shape[0], lb=0, name='d_var')
    
    eor_par = pd.DataFrame(data=0,index=si_data.index,columns=['EOR'])
    eor_par.loc[(si_data['DSA']==0).values,'EOR'] = 1

    changing_rate_storage = (changing_rate.loc[changing_rate['Year']==yr_end,'Comitted capture (Mt)'].values/changing_rate.loc[changing_rate['Year']==2021,'Comitted capture (Mt)'].values)[0]**np.log2((1-0.0715))
    cos_site2 = cos_site*changing_rate_storage
    
    Potential_site = cos_site2*i_par['StorageConstruct'].values

    oil_changing = pd.read_excel('../input/dict/Dict_OilPrice.xlsx',usecols=['Year','Oil price'])
    oil_changing = (oil_changing.loc[oil_changing['Year']==yr_end,'Oil price'].values/oil_changing.loc[oil_changing['Year']==2020,'Oil price'].values)[0]
    
    cos_stor2 = cos_stor*changing_rate_storage
    ben_eor2 = ben_eor*oil_changing
    Potential_sto = cos_stor2-ben_eor2*eor_par['EOR'].values
    
    
    del oil_changing,changing_rate_storage
    
    filter1 = np.isin(si_data['Plant ID'],all_loc['Plant ID'])
    m.addConstr(sum(i_var.select(np.where(~filter1)[0]))==0,name='Si_Zero')
    del filter1
    
    if yr_beg != startyr:
        filter1 = (i_par['StorageConstruct']==0)
        for iloc in np.where(filter1)[0]:
            m.addConstr(i_var[iloc]==1,name='Si_inj'+str(iloc))
        try:
            del filter1,iloc
        except:
            pass
        
    
    #%%
    b_var1 = m.addVars(edge_distance.shape[0], vtype=GRB.BINARY, name='b_var1')
    b_var2 = m.addVars(edge_distance.shape[0], vtype=GRB.BINARY, name='b_var2')
    b_var3 = m.addVars(edge_distance.shape[0], vtype=GRB.BINARY, name='b_var3')
    b_var4 = m.addVars(edge_distance.shape[0], vtype=GRB.BINARY, name='b_var4')
    
    changing_rate_trans = (changing_rate.loc[changing_rate['Year']==yr_end,'Comitted capture (Mt)'].values/changing_rate.loc[changing_rate['Year']==2021,'Comitted capture (Mt)'].values)[0]**np.log2((1-0.05))
    
    cos_pipe2 = [changing_rate_trans*i for i in cos_pipe]
    cos_tran2 = [changing_rate_trans*i for i in cos_tran]
    
    Potential_pip1 = cos_pipe2[0]*edge_distance['PiplineInstall1'].values
    Potential_tran1 = cos_tran2[0]*edge_distance['Distance'].values
    Potential_pip2 = cos_pipe2[1]*edge_distance['PiplineInstall2'].values*edge_distance['Distance'].values
    Potential_tran2 = cos_tran2[1]*edge_distance['Distance'].values
    Potential_pip3 = cos_pipe2[2]*edge_distance['PiplineInstall3'].values*edge_distance['Distance'].values
    Potential_tran3 = cos_tran2[2]*edge_distance['Distance'].values
    Potential_pip4 = cos_pipe2[3]*edge_distance['PiplineInstall4'].values*edge_distance['Distance'].values
    Potential_tran4 = cos_tran2[3]*edge_distance['Distance'].values
    
    del changing_rate_trans
    
    #%%
    t_var = m.addVars(edge_distance.shape[0], lb=0, name='t_var')
    t_para= edge_distance.loc[:,['Start','End']].copy(deep=True)
    
    for i in range(all_loc.shape[0]):
        print(all_loc.loc[i,'Plant ID'], flush=True)
        
        t_para2 = t_para.copy(deep=True)
        t_para2 = t_para2.reindex(columns=['Start','End',all_loc.loc[i,'Plant ID']],fill_value=np.nan)
        
        filter1 = (t_para2['Start']!=all_loc.loc[i,'Plant ID'])&(t_para2['End']!=all_loc.loc[i,'Plant ID'])
        t_para2.loc[filter1,all_loc.loc[i,'Plant ID']] = 0
        filter2 = (t_para2['Start']!=all_loc.loc[i,'Plant ID'])&(t_para2['End']==all_loc.loc[i,'Plant ID'])
        t_para2.loc[filter2,all_loc.loc[i,'Plant ID']] = 1
        filter3 = (t_para2['Start']==all_loc.loc[i,'Plant ID'])&(t_para2['End']!=all_loc.loc[i,'Plant ID'])
        t_para2.loc[filter3,all_loc.loc[i,'Plant ID']] = -1
        
        try:
            if 'Si_' in all_loc.loc[i,'Plant ID']:
                loc = np.where(si_data['Plant ID']==all_loc.loc[i,'Plant ID'])[0][0]
                if t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].shape[0] == 0:
                    m.addConstr(d_var[loc]==0,name='Q'+str(i))
                else:
                    m.addConstr((t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].values@t_var.select(np.where(~filter1)[0]))-(d_var[loc])==0,name='Q'+str(i))
            else:
                loc = np.where(so_data['Plant ID']==all_loc.loc[i,'Plant ID'])[0][0]
                if t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].shape[0] == 0:
                    m.addConstr(s_var[loc]==0,name='Q'+str(i))
                else:
                    m.addConstr((t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].values@t_var.select(np.where(~filter1)[0])+(so_data.loc[loc,'Commitment (Mt)']*0.9*s_var[loc])==0),name='Q'+str(i))
        except:
            if len(np.where(~filter1)[0]) != 0:
                m.addConstr((t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].values@t_var.select(np.where(~filter1)[0]))==0,name='Q'+str(i))
            else:
                continue
        
    del filter1,filter2,filter3,loc,t_para2,t_para
    
    
    #%%
    m.addConstr((so_data['Commitment (Mt)'].values*0.9).T@list(s_var.values())>=constrain,name='E1')
    m.addConstr((so_data['Commitment (Mt)'].values*0.9).T@list(s_var.values())<=constrain*1.01,name='E2')
    
    for i in range(si_data.shape[0]):
        m.addConstr(d_var[i]<=i_var[i]*si_data.loc[i,'CO2'],name='I'+str(i))
                
    t_var1 = m.addVars(edge_distance.shape[0], lb=0, name='t_var1')
    t_var2 = m.addVars(edge_distance.shape[0], lb=0, name='t_var2')
    t_var3 = m.addVars(edge_distance.shape[0], lb=0, name='t_var3')
    t_var4 = m.addVars(edge_distance.shape[0], lb=0, name='t_var4')
    
    m.addConstrs((t_var1[i]<=b_var1[i]*pipline_cap[0] for i in range(edge_distance.shape[0])),name='P1')
    m.addConstrs((t_var2[i]<=b_var2[i]*pipline_cap[1] for i in range(edge_distance.shape[0])),name='P2')
    m.addConstrs((t_var3[i]<=b_var3[i]*pipline_cap[2] for i in range(edge_distance.shape[0])),name='P3')
    m.addConstrs((t_var4[i]<=b_var4[i]*pipline_cap[3] for i in range(edge_distance.shape[0])),name='P4')
    m.addConstrs((t_var1[i]+t_var2[i]+t_var3[i]+t_var4[i]==t_var[i] for i in range(edge_distance.shape[0])),name='P5')
    m.addConstrs((b_var1[i]+b_var2[i]+b_var3[i]+b_var4[i]<=1 for i in range(edge_distance.shape[0])),name='P6')

    
    if yr_beg != startyr:
        filter1 = (edge_distance['PiplineInstall1']==0)
        for iloc in np.where(filter1)[0]:
            m.addConstr(b_var1[iloc]==1,name='Pi_ins1'+str(iloc))
        try:
            del filter1,iloc
        except:
            pass
            
        filter1 = (edge_distance['PiplineInstall2']==0)
        for iloc in np.where(filter1)[0]:
            m.addConstr(b_var2[iloc]==1,name='Pi_ins2'+str(iloc))
        try:
            del filter1,iloc
        except:
            pass
            
        filter1 = (edge_distance['PiplineInstall3']==0)
        for iloc in np.where(filter1)[0]:
            m.addConstr(b_var3[iloc]==1,name='Pi_ins3'+str(iloc))
        try:
            del filter1,iloc
        except:
            pass
            
        filter1 = (edge_distance['PiplineInstall4']==0)
        for iloc in np.where(filter1)[0]:
            m.addConstr(b_var4[iloc]==1,name='Pi_ins4'+str(iloc))
        try:
            del filter1,iloc
        except:
            pass
        
    
    #%%
    m.setParam('MIPGap',0.01)
    m.setParam('IntegralityFocus',1)
    m.setParam('verbose',True)
    m.setParam('ModelSense', 0)
    m.setParam('BranchDir', 1)
    m.setParam('Heuristics', 0.5)
    m.setParam('LPWarmStart',2)
    m.setParam('Threads', 8)

    m.setObjective(0,sense=GRB.MINIMIZE)
    m.optimize()
    
    m.setObjective(((Potential_rot+Potential_cap)/10**8 @ list(s_var.values()))+\
                    (Potential_site/10**8 @ list(i_var.values())+Potential_sto/10**8 @ list(d_var.values()))+\
                    (Potential_tran1/10**8 @ list(t_var1.values())+Potential_tran2/10**8 @ list(t_var2.values())+\
                     Potential_tran3/10**8 @ list(t_var3.values())+Potential_tran4/10**8 @ list(t_var4.values())+\
                     Potential_pip1/10**8 @ list(b_var1.values())+Potential_pip2/10**8 @ list(b_var2.values())+\
                     Potential_pip3/10**8 @ list(b_var3.values())+Potential_pip4/10**8 @ list(b_var4.values())),sense=GRB.MINIMIZE)
    m.optimize()

    b = time.time()
    
    # #保存源数据
    cap_data = so_data.copy(deep=True)
    cap_data.insert(loc=cap_data.shape[1],value=m.getAttr('x', s_var).select('*', '*'),column='Capature Marker')
    # cap_data.insert(loc=cap_data.shape[1],value=yr_end,column='Year')
    cap_data['CO2 Capature (Mt)'] = cap_data['Capature Marker']*cap_data['Commitment (Mt)']*0.9
    cap_data['Capature Cost (USD)'] = (Potential_rot+Potential_cap) * cap_data['Capature Marker']
    cap_data.to_csv(SSM_dir+'/capture_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
        
    pipe_data = edge_distance.copy(deep=True)
    pipe_data.insert(loc=pipe_data.shape[1],value=m.getAttr('x', b_var1).select('*', '*'),column='Pipeline1 Marker')
    pipe_data.insert(loc=pipe_data.shape[1],value=m.getAttr('x', b_var2).select('*', '*'),column='Pipeline2 Marker')
    pipe_data.insert(loc=pipe_data.shape[1],value=m.getAttr('x', b_var3).select('*', '*'),column='Pipeline3 Marker')
    pipe_data.insert(loc=pipe_data.shape[1],value=m.getAttr('x', b_var4).select('*', '*'),column='Pipeline4 Marker')
    pipe_data.insert(loc=pipe_data.shape[1],value=m.getAttr('x', t_var1).select('*', '*'),column='CO2 Transport1 (Mt)')
    pipe_data.insert(loc=pipe_data.shape[1],value=m.getAttr('x', t_var2).select('*', '*'),column='CO2 Transport2 (Mt)')
    pipe_data.insert(loc=pipe_data.shape[1],value=m.getAttr('x', t_var3).select('*', '*'),column='CO2 Transport3 (Mt)')
    pipe_data.insert(loc=pipe_data.shape[1],value=m.getAttr('x', t_var4).select('*', '*'),column='CO2 Transport4 (Mt)')
    pipe_data.insert(loc=pipe_data.shape[1],value=m.getAttr('x', t_var).select('*', '*'),column='CO2 Transport (Mt)')
    pipe_data.insert(loc=pipe_data.shape[1],value=yr_end,column='Year')
    
    Potential_pip = Potential_pip1*pipe_data['Pipeline1 Marker']+Potential_pip2*pipe_data['Pipeline2 Marker']+\
                    Potential_pip3*pipe_data['Pipeline3 Marker']+Potential_pip4*pipe_data['Pipeline4 Marker']
        
    Potential_tran = Potential_tran1*pipe_data['CO2 Transport1 (Mt)']+Potential_tran2*pipe_data['CO2 Transport2 (Mt)']+\
                      Potential_tran3*pipe_data['CO2 Transport3 (Mt)']+Potential_tran4*pipe_data['CO2 Transport4 (Mt)']
                    
    pipe_data['Transport Cost (USD)'] = (Potential_tran+Potential_pip)
    pipe_data.to_csv(SSM_dir+'/transport_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    sto_data = si_data.copy(deep=True)
    sto_data.insert(loc=sto_data.shape[1],value=m.getAttr('x', i_var).select('*', '*'),column='Storage Marker')
    sto_data.insert(loc=sto_data.shape[1],value=m.getAttr('x', d_var).select('*', '*'),column='CO2 Storage (Mt)')
    sto_data.insert(loc=sto_data.shape[1],value=yr_end,column='Year')
    sto_data['Storage Cost (USD)'] = Potential_sto*sto_data['CO2 Storage (Mt)']\
        +Potential_site*sto_data['Storage Marker']
    sto_data.to_csv(SSM_dir+'/store_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
        
    return

#%%
def SSM_main(ieng_sc,iend_sc,ior_sc,ireg,
             NewFuelEmis_dir,SSM_dir,Turnover_dir,
             yr_beg,yr_end):
    
    si_data,so_si = get_sink(ireg=ireg,yr_beg=yr_beg,yr_end=yr_end,
                             SSM_dir=SSM_dir)
    so_data,constrain = get_source(ieng_sc=ieng_sc,ior_sc=ior_sc,
                                   so_si=so_si,
                                   NewFuelEmis_dir=NewFuelEmis_dir,Turnover_dir=Turnover_dir,SSM_dir=SSM_dir,
                                   ireg=ireg,yr_beg=yr_beg,yr_end=yr_end)

    edge_distance,all_loc = get_edge(so_data=so_data,si_data=si_data,SSM_dir=SSM_dir,
                                     ireg=ireg,yr_beg=yr_beg,yr_end=yr_end)
    
    SSM_drive(so_data=so_data,si_data=si_data,
              edge_distance=edge_distance,all_loc=all_loc,
              ireg=ireg,yr_beg=yr_beg,yr_end=yr_end,
              constrain=constrain,SSM_dir=SSM_dir)
    
    return

#%%
if __name__ == "__main__":
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Cost'
    ireg='India'
    NewFuelEmis_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/3_NewFuelEmis/'
    SSM_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/4_SSM/'
    Turnover_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/0_Turnover/'
    
    mkdir(SSM_dir)
    yr_beg=2040
    yr_end=2050
    
    SSM_main(ieng_sc,iend_sc,ior_sc,ireg,
             NewFuelEmis_dir,SSM_dir,Turnover_dir,
             yr_beg,yr_end)