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
def get_sink(ireg,SSM_dir,yr_beg,yr_end):
    si_data = pd.read_csv(SSM_dir+'/store_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',
                          usecols=['Plant ID','Type','Sector','DSA','Longitude',
                                   'Latitude','CO2','Capacity','geometry','CO2 Storage (Mt)'])
    si_data = si_data.loc[si_data['CO2 Storage (Mt)']>0,:].reset_index(drop=True)
    si_data.drop(columns=['CO2 Storage (Mt)'],inplace=True)
    
    return si_data

def get_source(SSM_dir,ireg,yr_beg,yr_end):
    df = pd.read_csv(SSM_dir+'/capture_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    df = df.loc[df['CO2 Capature (Mt)']>0,:].reset_index(drop=True)
    df.drop(columns=['CO2 Capature (Mt)','Capature Marker','Capature Cost (USD)'],inplace=True)
    constrain = df['Commitment (Mt)'].sum()*0.9
    
    return df,constrain

def get_edge(so_data,si_data,SSM_dir,ireg,yr_beg,yr_end):
    all_dis2 = pd.read_csv(SSM_dir+'/transport_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',
                           usecols=['Start', 'Lon_st', 'Lat_st', 'End',
                                    'Lon_en', 'Lat_en', 'Distance','CO2 Transport (Mt)'])
    all_dis2 = all_dis2.loc[all_dis2['CO2 Transport (Mt)']>0,:].reset_index(drop=True)
    all_dis2.drop(columns=['CO2 Transport (Mt)'],inplace=True)
    
    all_loc = pd.concat([so_data.loc[:,['Plant ID']],si_data.loc[:,['Plant ID']],
                         all_dis2.loc[:,['Start']].rename(columns={'Start':'Plant ID'}),
                         all_dis2.loc[:,['End']].rename(columns={'End':'Plant ID'})],axis=0)
    
    all_loc = all_loc.drop_duplicates().reset_index(drop=True)
    
    return all_dis2,all_loc

def SSM_drive(so_data,si_data,
              edge_distance,all_loc,
              ireg,yr_beg,yr_end,
              constrain,SSM_dir):
    
    m = Model()
    
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
        
        i_par = pd.DataFrame(data=1,index=si_data.index,columns=['StorageConstruct'])
        filtered_id = i_par_all.loc[i_par_all['StorageConstruct']==0,'Plant ID']
        i_par.loc[np.isin(si_data['Plant ID'],filtered_id),'StorageConstruct'] = 0
        
        del edge_distance_all,r_par_all,i_par_all

    cos_ret = pd.read_csv('../../2_GetPPHarmonized/output/PlantCaptureCost.csv')
    cos_ret = pd.merge(so_data.loc[:,['Plant ID','Sector','Facility Type']],cos_ret,on='Plant ID',how='left')
    
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

    Potential_rot = (cos_ret*r_par['CCUSInstall'].values)
    
    cos_cap2 = cos_cap
    Potential_cap = cos_cap2*so_data['Commitment (Mt)'].values*0.9#(cos_cap-ben_cp)

    pool_data = so_data.loc[:,['Pool_ID','WaterResou','WaterOrigi']].drop_duplicates()
    for ipool in pool_data['Pool_ID']:
        wr = pool_data.loc[pool_data['Pool_ID']==ipool,'WaterResou'].values[0]
        wod = pool_data.loc[pool_data['Pool_ID']==ipool,'WaterOrigi'].values[0]

        if wod/wr<=0.8:
            constr_expr = quicksum(
                s_var[i] * waterconsum * so_data.loc[i, 'CO2 Emissions'] * 0.9/10**6#年耗水量统一为 10^6 m3
                for i in range(so_data.shape[0])
                if so_data.loc[i, 'Pool_ID'] == ipool
            )

            m.addConstr((constr_expr + wod) / wr <= 0.8, name=f'WaterLimit_{ipool}')
        else:
            constr_expr = quicksum(
                s_var[i]
                for i in range(so_data.shape[0])
                if so_data.loc[i, 'Pool_ID'] == ipool
            )

            m.addConstr(constr_expr == 0, name=f'WaterLimit_{ipool}')

    del pool_data,wr,wod,constr_expr

    filter1 = np.isin(so_data['Plant ID'],all_loc['Plant ID'])
    m.addConstr(sum(s_var.select(np.where(~filter1)[0]))==0,name='So_Zero')
    del filter1

    #%%
    i_var = m.addVars(si_data.shape[0], vtype=GRB.BINARY, name='i_var')
    d_var = m.addVars(si_data.shape[0], lb=0, name='d_var')

    eor_par = pd.DataFrame(data=0,index=si_data.index,columns=['EOR'])
    eor_par.loc[(si_data['DSA']==0).values,'EOR'] = 1
    
    cos_site2 = cos_site

    Potential_site = cos_site2*i_par['StorageConstruct'].values
    oil_changing = pd.read_excel('../input/dict/Dict_OilPrice.xlsx',usecols=['Year','Oil price'])
    oil_changing = (oil_changing.loc[oil_changing['Year']==yr_end,'Oil price'].values/oil_changing.loc[oil_changing['Year']==2020,'Oil price'].values)[0]
    
    cos_stor2 = cos_stor
    ben_eor2 = ben_eor*oil_changing
    Potential_sto = cos_stor2-ben_eor2*eor_par['EOR'].values
    
    del oil_changing

    filter1 = np.isin(si_data['Plant ID'],all_loc['Plant ID'])
    m.addConstr(sum(i_var.select(np.where(~filter1)[0]))==0,name='Si_Zero')
    del filter1

    #%%
    b_var1 = m.addVars(edge_distance.shape[0], vtype=GRB.BINARY, name='b_var1')
    b_var2 = m.addVars(edge_distance.shape[0], vtype=GRB.BINARY, name='b_var2')
    b_var3 = m.addVars(edge_distance.shape[0], vtype=GRB.BINARY, name='b_var3')
    b_var4 = m.addVars(edge_distance.shape[0], vtype=GRB.BINARY, name='b_var4')

    cos_pipe2 = cos_pipe
    cos_tran2 = cos_tran
    
    Potential_pip1 = cos_pipe2[0]*edge_distance['PiplineInstall1'].values
    Potential_tran1 = cos_tran2[0]*edge_distance['Distance'].values
    Potential_pip2 = cos_pipe2[1]*edge_distance['PiplineInstall2'].values*edge_distance['Distance'].values
    Potential_tran2 = cos_tran2[1]*edge_distance['Distance'].values
    Potential_pip3 = cos_pipe2[2]*edge_distance['PiplineInstall3'].values*edge_distance['Distance'].values
    Potential_tran3 = cos_tran2[2]*edge_distance['Distance'].values
    Potential_pip4 = cos_pipe2[3]*edge_distance['PiplineInstall4'].values*edge_distance['Distance'].values
    Potential_tran4 = cos_tran2[3]*edge_distance['Distance'].values
    
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
        
        if 'Si_' in all_loc.loc[i,'Plant ID']:
            if si_data.loc[si_data['Plant ID']==all_loc.loc[i,'Plant ID'],'Plant ID'].shape[0]==0:
                m.addConstr((t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].values@t_var.select(np.where(~filter1)[0]))==0,name='Q'+str(i))
            else:
                loc = np.where(si_data['Plant ID']==all_loc.loc[i,'Plant ID'])[0][0]
                if t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].shape[0] == 0:
                    m.addConstr(d_var[loc]==0,name='Q'+str(i))
                else:
                    m.addConstr((t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].values@t_var.select(np.where(~filter1)[0]))-(d_var[loc])==0,name='Q'+str(i))
        else:
            if so_data.loc[so_data['Plant ID']==all_loc.loc[i,'Plant ID'],'Plant ID'].shape[0]==0:
                m.addConstr((t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].values@t_var.select(np.where(~filter1)[0])==0),name='Q'+str(i))
            else:
                loc = np.where(so_data['Plant ID']==all_loc.loc[i,'Plant ID'])[0][0]
                if t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].shape[0] == 0:
                    m.addConstr(s_var[loc]==0,name='Q'+str(i))
                else:
                    m.addConstr((t_para2.loc[~filter1,all_loc.loc[i,'Plant ID']].values@t_var.select(np.where(~filter1)[0])+(so_data.loc[loc,'Commitment (Mt)']*0.9*s_var[loc])==0),name='Q'+str(i))
    
    del filter1,filter2,filter3,loc,t_para2,t_para
    
    #%%
    m.addConstr((so_data['Commitment (Mt)'].values*0.9).T@list(s_var.values())>=constrain,name='E')

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

    iloc_ls = []
    for i in range(edge_distance.shape[0]):
        if i in iloc_ls:
            continue

        iloc = np.where((edge_distance['Start']==edge_distance.loc[i,'End'])&(edge_distance['End']==edge_distance.loc[i,'Start']))[0]
        if len(iloc)==0:
            m.addConstr((b_var1[i]+b_var2[i]+b_var3[i]+b_var4[i]<=1),name='P6'+str(i))
        
        elif len(iloc)>0:
            iloc_ls.append(iloc[0])
            m.addConstr((b_var1[i]+b_var2[i]+b_var3[i]+b_var4[i]+b_var1[iloc[0]]+b_var2[iloc[0]]+b_var3[iloc[0]]+b_var4[iloc[0]]<=1),name='P6'+str(i))
        
    del iloc_ls,iloc,i

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
    m.setParam('MIPGap',1*10**(-6))
    m.setParam('IntegralityFocus',1)
    m.setParam('verbose',True)
    m.setParam('ModelSense', 0)
    m.setParam('BranchDir', 1)
    m.setParam('Heuristics', 0.5)
    m.setParam('LPWarmStart',2)
    #m.setParam('Threads', 12)

    m.setObjective(0,sense=GRB.MINIMIZE)
    m.optimize()

    m.setObjective(((Potential_rot+Potential_cap)/10**8 @ list(s_var.values()))+\
                    (Potential_site/10**8 @ list(i_var.values())+Potential_sto/10**8 @ list(d_var.values()))+\
                    (Potential_tran1/10**8 @ list(t_var1.values())+Potential_tran2/10**8 @ list(t_var2.values())+\
                     Potential_tran3/10**8 @ list(t_var3.values())+Potential_tran4/10**8 @ list(t_var4.values())+\
                     Potential_pip1/10**8 @ list(b_var1.values())+Potential_pip2/10**8 @ list(b_var2.values())+\
                     Potential_pip3/10**8 @ list(b_var3.values())+Potential_pip4/10**8 @ list(b_var4.values())),sense=GRB.MINIMIZE)
    m.optimize()
    
    cap_data = so_data.copy(deep=True)
    cap_data.insert(loc=cap_data.shape[1],value=m.getAttr('x', s_var).select('*', '*'),column='Capature Marker')
    cap_data['CO2 Capature (Mt)'] = cap_data['Capature Marker']*cap_data['Commitment (Mt)']*0.9
    cap_data['Capature Cost (USD)'] = (Potential_rot+Potential_cap) * cap_data['Capature Marker']
    print('捕获成本：'+str(cap_data['Capature Cost (USD)'].sum()/cap_data['CO2 Capature (Mt)'].sum()/10**6))
    cap_data.to_csv(SSM_dir+'/capture_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)

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
    pipe_data.to_csv(SSM_dir+'/transport_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    sto_data = si_data.copy(deep=True)
    sto_data.insert(loc=sto_data.shape[1],value=m.getAttr('x', i_var).select('*', '*'),column='Storage Marker')
    sto_data.insert(loc=sto_data.shape[1],value=m.getAttr('x', d_var).select('*', '*'),column='CO2 Storage (Mt)')
    sto_data.insert(loc=sto_data.shape[1],value=yr_end,column='Year')
    sto_data['Storage Cost (USD)'] = Potential_sto*sto_data['CO2 Storage (Mt)']\
        +Potential_site*sto_data['Storage Marker']
    sto_data.to_csv(SSM_dir+'/store_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    if yr_beg == startyr:
        edge_distance['PiplineInstall1'] = pipe_data['Pipeline1 Marker'].replace({0:1,1:0})
        edge_distance['PiplineInstall2'] = pipe_data['Pipeline2 Marker'].replace({0:1,1:0})
        edge_distance['PiplineInstall3'] = pipe_data['Pipeline3 Marker'].replace({0:1,1:0})
        edge_distance['PiplineInstall4'] = pipe_data['Pipeline4 Marker'].replace({0:1,1:0})
        
        edge_distance['Year'] = yr_end
        edge_distance.to_csv(SSM_dir+'/transport_sec_'+ireg+'_marker.csv',index=None)

        r_par['CCUSInstall'] = r_par['CCUSInstall'].replace({0:1,1:0})
        r_par['Year'] = yr_end
        r_par['Plant ID'] = so_data['Plant ID'].values
        r_par = r_par.reindex(columns=['Plant ID','Year','CCUSInstall'])
        r_par.to_csv(SSM_dir+'/capture_sec_'+ireg+'_marker.csv',index=None)

        i_par['StorageConstruct'] = i_par['StorageConstruct'].replace({0:1,1:0})
        i_par['Year'] = yr_end
        i_par['Plant ID'] = si_data['Plant ID'].values
        i_par = i_par.reindex(columns=['Plant ID','Year','StorageConstruct'])
        i_par.to_csv(SSM_dir+'/store_sec_'+ireg+'_marker.csv',index=None)
        
    else:
        edge_distance_all = pd.read_csv(SSM_dir+'/transport_sec_'+ireg+'_marker.csv')
        
        edge_distance['PiplineInstall1'] = pipe_data['Pipeline1 Marker'].replace({0:1,1:0})
        edge_distance['PiplineInstall2'] = pipe_data['Pipeline2 Marker'].replace({0:1,1:0})
        edge_distance['PiplineInstall3'] = pipe_data['Pipeline3 Marker'].replace({0:1,1:0})
        edge_distance['PiplineInstall4'] = pipe_data['Pipeline4 Marker'].replace({0:1,1:0})
                
        edge_distance['Year'] = yr_end
        
        edge_distance_all = pd.concat([edge_distance_all,edge_distance],axis=0)
        edge_distance_all.reset_index(drop=True,inplace=True)
        edge_distance_all.to_csv(SSM_dir+'/transport_sec_'+ireg+'_marker.csv',index=None)

        r_par_all = pd.read_csv(SSM_dir+'/capture_sec_'+ireg+'_marker.csv')
        
        r_par['CCUSInstall'] = cap_data['Capature Marker'].replace({0:1,1:0})
        r_par['Year'] = yr_end
        r_par['Plant ID'] = so_data['Plant ID'].values
        r_par = r_par.reindex(columns=['Plant ID','Year','CCUSInstall'])
        
        r_par_all = pd.concat([r_par_all,r_par],axis=0)
        r_par_all.reset_index(drop=True,inplace=True)
        r_par_all.to_csv(SSM_dir+'/capture_sec_'+ireg+'_marker.csv',index=None)

        i_par_all = pd.read_csv(SSM_dir+'/store_sec_'+ireg+'_marker.csv')
        
        i_par['StorageConstruct'] = sto_data['Storage Marker'].replace({0:1,1:0})
        i_par['Year'] = yr_end
        i_par['Plant ID'] = si_data['Plant ID'].values
        i_par = i_par.reindex(columns=['Plant ID','Year','StorageConstruct'])
        
        i_par_all= pd.concat([i_par_all,i_par],axis=0)
        i_par_all.reset_index(drop=True,inplace=True)
        i_par_all.to_csv(SSM_dir+'/store_sec_'+ireg+'_marker.csv',index=None)
    
    return

#%%
def SSM_sec_main(ieng_sc,iend_sc,ior_sc,ireg,NewFuelEmis_dir,SSM_dir,yr_beg,yr_end):
    si_data = get_sink(ireg=ireg,SSM_dir=SSM_dir,yr_beg=yr_beg,yr_end=yr_end)
    so_data,constrain = get_source(SSM_dir=SSM_dir,
                                   ireg=ireg,
                                   yr_beg=yr_beg,yr_end=yr_end)
    
    edge_distance,all_loc = get_edge(so_data=so_data,si_data=si_data,
                                     SSM_dir=SSM_dir,
                                     ireg=ireg,
                                     yr_beg=yr_beg,yr_end=yr_end)
    
    SSM_drive(so_data=so_data,si_data=si_data,
              edge_distance=edge_distance,all_loc=all_loc,
              ireg=ireg,yr_beg=yr_beg,yr_end=yr_end,
              constrain=constrain,SSM_dir=SSM_dir)
    
    return

#%%
if __name__ == "__main__":
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Age'
    ireg='India'
    
    NewFuelEmis_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/3_NewFuelEmis/'
    SSM_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/4_SSM/'
    
    mkdir(SSM_dir)
    
    yr_beg=2020
    yr_end=2030
    
    SSM_sec_main(ieng_sc,iend_sc,ior_sc,ireg,NewFuelEmis_dir,SSM_dir,yr_beg,yr_end)