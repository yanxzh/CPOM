# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 13:50:36 2024

@author: 92978
"""

#%%
import networkx as nx
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
from matplotlib import cm
import matplotlib as mpl
mpl.rcParams['font.sans-serif'] = ["STSong"];
mpl.rcParams["axes.unicode_minus"] = False;
plt.rc('font',size=50,family="Arial");
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
from S1_Global_ENV import *

#%%
def tracking_source(cap_data,pipe_data,sto_data):

    flow_data = pd.DataFrame()
    loop_mr = 0
    
    pipe_data1 = pipe_data.copy(deep=True)
    cap_ls = []
    while pipe_data1.shape[0]>0:
        single_in = pipe_data1.loc[np.isin(pipe_data1['Start'],pipe_data1['End'])==0,:]
        single_in = single_in.reset_index(drop=True)
        
        if loop_mr == 0:
            sing_flow = single_in.loc[:,['Start','End','CO2 Transport (Mt)']]
            sing_flow.columns = ['Layer1','Layer2','CO2 Transport (Mt)']
            
            flow_data = pd.concat([flow_data,sing_flow],axis=0).reset_index(drop=True)
            flow_data2 = flow_data.copy(deep=True)
            
        else:
            for i in range(flow_data.shape[0]):
                if i == 31:
                    pass

                flow_mix = pipe_data.loc[pipe_data['Start']==flow_data.loc[i,'Layer'+str(loop_mr+1)],['Start','End']+['CO2 Transport (Mt)']]
                flow_mix = flow_mix.reset_index(drop=True)
            
                if flow_mix.shape[0]==0:
                    continue

                sto_this = pd.DataFrame(columns=['Start','CO2 Transport (Mt)'])
                for ifl in range(flow_mix.shape[0]):
                    sto_nodes = sto_data.loc[sto_data['Plant ID']==flow_mix.loc[ifl,'Start'],['Plant ID','CO2']]
                    if (sto_nodes.shape[0]>0):
                        if np.isin(sto_nodes['Plant ID'].values[0],sto_this['Start'])==0:
                            sto_nodes.columns = ['Start','CO2 Transport (Mt)']
                            sto_this = pd.concat([sto_this,sto_nodes],axis=0)
                        
                sto_this = sto_this.drop_duplicates()
                flow_mix = pd.concat([flow_mix,sto_this],axis=0)
                    
                flow_mix.columns = ['Layer'+str(loop_mr+1),'Layer'+str(loop_mr+2),'CO2 Transport (Mt)']
                
                flow_mix['Mix'] = flow_mix['CO2 Transport (Mt)']/flow_mix['CO2 Transport (Mt)'].sum()
                
                single_in = flow_data.loc[[i],:]

                cap_nodes = cap_data.loc[cap_data['Plant ID']==flow_data.loc[i,'Layer'+str(loop_mr+1)],['Plant ID','CO2 Capature (Mt)']]
                if (cap_nodes.shape[0]>0) and (np.isin(cap_nodes['Plant ID'].values,cap_ls)==0):
                    cap_ls.append(cap_nodes['Plant ID'].values[0])
                    cap_nodes.columns = ['Layer'+str(loop_mr+1),'CO2 Transport (Mt)']
                    single_in = pd.concat([single_in,cap_nodes],axis=0)
                
                sing_flow = pd.merge(single_in,
                                     flow_mix.loc[:,['Layer'+str(loop_mr+1),'Layer'+str(loop_mr+2),'Mix']],
                                     on='Layer'+str(loop_mr+1),how='outer')
                sing_flow['CO2 Transport (Mt)'] = sing_flow['CO2 Transport (Mt)']*sing_flow['Mix'].values
                
                sing_flow.drop(['Mix'],axis=1,inplace=True)
                
                flow_data2 = flow_data2.loc[(flow_data2.index!=i),:]
                sing_flow.index = sing_flow.index+max(flow_data2.index)+1
                flow_data2 = pd.concat([flow_data2,sing_flow],axis=0)
            
            flow_data = flow_data2.reset_index(drop=True).copy(deep=True)
            flow_data2 = flow_data.copy(deep=True)
                
        pipe_data1 = pipe_data1.loc[np.isin(pipe_data1['Start'],pipe_data1['End'])!=0,:]
        pipe_data1 = pipe_data1.reset_index(drop=True)
    
        print(loop_mr)
        loop_mr += 1
    
    flow_data = flow_data.reset_index(drop=True)
    
    return flow_data,loop_mr+2

def final_deal(df,num):
    df2 = df.loc[:,['Layer'+str(i) for i in range(1,num)]]
    for iin in range(df.shape[0]):
        if np.where(pd.isnull(df.loc[iin,['Layer'+str(i) for i in range(1,num)]])==0)[0][0]==0:
            continue
        else:
            nan_loc = np.where(pd.isnull(df.loc[iin,:])==0)[0][0]
            df2.loc[iin,:] = df.loc[iin,['Layer'+str(i) for i in range(1,num)]].shift(-nan_loc).values
            
    df2['CO2 Transport (Mt)'] = df['CO2 Transport (Mt)']
    
    return df2

#%%
def FlowTrace_main(ieng_sc,iend_sc,ior_sc,ireg,
                  SSM_dir,NetworkStatus_dir,
                  yr_beg,yr_end):
    

    cap_data = pd.read_csv(SSM_dir+'/capture_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    pipe_data = pd.read_csv(SSM_dir+'/transport_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    sto_data = pd.read_csv(SSM_dir+'/store_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    
    cap_data = cap_data.loc[(cap_data['Year']==yr_end)&(cap_data['CO2 Capature (Mt)']>0),:].reset_index(drop=True)
    pipe_data = pipe_data.loc[(pipe_data['Year']==yr_end)&(pipe_data['CO2 Transport (Mt)']>0),:].reset_index(drop=True)
    sto_data = sto_data.loc[(sto_data['Year']==yr_end)&(sto_data['CO2 Storage (Mt)']>0),:].reset_index(drop=True)


    flow_all,layerNum = tracking_source(cap_data=cap_data,pipe_data=pipe_data,sto_data=sto_data)

    flow_all = final_deal(df=flow_all,num=layerNum)
    flow_all = final_deal(df=flow_all,num=layerNum)
    flow_all.to_csv(NetworkStatus_dir+'/4_CostAndFlow/3_AllFlowData_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)
    
    flow_all2 = flow_all.loc[:,['Layer1','CO2 Transport (Mt)']].groupby(['Layer1'],as_index=False).sum()
    flow_all2.to_csv(NetworkStatus_dir+'/4_CostAndFlow/3_AllFlowData_PlantCapaturetest_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv',index=None)

    return

#%%
if __name__ == "__main__":
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Age'
    ireg='India'
    yr_beg=2040
    yr_end=2050
    
    SSM_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/4_SSM/'
    NetworkStatus_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/5_Network_'+str(yr_end)+'/'
    
    mkdir(SSM_dir)
    mkdir(NetworkStatus_dir)
    
    FlowTrace_main(ieng_sc,iend_sc,ior_sc,ireg,SSM_dir,NetworkStatus_dir,yr_beg,yr_end)