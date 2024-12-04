# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 13:53:31 2024

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
def tracking_source(cap_data,pipe_data,sto_data,NetworkStatus_dir,ireg):
    mkdir(NetworkStatus_dir+'/4_CostAndFlow/')
    
    flow_data = pd.DataFrame()
    loop_mr = 0
    
    pipe_data1 = pipe_data.copy(deep=True)
    while pipe_data1.shape[0]>0:
        single_in = pipe_data1.loc[np.isin(pipe_data1['Start'],pipe_data1['End'])==0,:]
        single_in = single_in.reset_index(drop=True)
        
        if loop_mr == 0:
            sing_flow = single_in.loc[:,['Start','End','Start','CO2 Transport (Mt)']]
            sing_flow.columns = ['Start','End','Source','CO2 Transport (Mt)']
            
            flow_data = pd.concat([flow_data,sing_flow],axis=0)
            
        else:
            for i in range(single_in.shape[0]):
                flow_mix = flow_data.loc[flow_data['End']==single_in.loc[i,'Start'],['Source','CO2 Transport (Mt)']]
                # if flow_mix.shape[0]>1:
                #     continue
                
                cap_nodes = cap_data.loc[cap_data['Plant ID']==single_in.loc[i,'Start'],['Plant ID','CO2 Capature (Mt)']]
                if cap_nodes.shape[0]>0:
                    cap_nodes.columns = ['Source','CO2 Transport (Mt)']
                    flow_mix = pd.concat([flow_mix,cap_nodes],axis=0)
                    
                flow_mix = flow_mix.groupby(['Source'],as_index=False).sum()
                flow_mix['Mix'] = flow_mix['CO2 Transport (Mt)']/flow_mix['CO2 Transport (Mt)'].sum()
                
                sing_flow = single_in.loc[np.repeat(i,flow_mix.shape[0]),['Start','End','Start','CO2 Transport (Mt)']]
                sing_flow.columns = ['Start','End','Source','CO2 Transport (Mt)']
                sing_flow['Source'] = flow_mix['Source'].values
                sing_flow['CO2 Transport (Mt)'] = sing_flow['CO2 Transport (Mt)']*flow_mix['Mix'].values
                
                flow_data = pd.concat([flow_data,sing_flow],axis=0)
                
                
        pipe_data1 = pipe_data1.loc[np.isin(pipe_data1['Start'],pipe_data1['End'])!=0,:]
        pipe_data1 = pipe_data1.reset_index(drop=True)
    
        
        print(loop_mr)
        loop_mr += 1
        
    flow_data.to_csv(NetworkStatus_dir+'/4_CostAndFlow/2_FlowData_sourceinfo_'+ireg+'.csv',index=None)
    
    sink_data = pd.DataFrame()
    loop_mr = 0
    
    for iso in range(sto_data.shape[0]):
        si_in = flow_data.loc[flow_data['End']==sto_data.loc[iso,'Plant ID'],['Source','CO2 Transport (Mt)']]
        si_in = si_in.groupby(['Source'],as_index=False).sum()
        si_in['Mix'] = si_in['CO2 Transport (Mt)']/si_in['CO2 Transport (Mt)'].sum()
        
        sink_flow = sto_data.loc[np.repeat(iso,si_in.shape[0]),['Plant ID','CO2 Storage (Mt)']]
        sink_flow['CO2 Storage (Mt)'] = sink_flow['CO2 Storage (Mt)']*si_in['Mix'].values
        sink_flow['Source'] = si_in['Source'].values
        
        sink_data = pd.concat([sink_data,sink_flow],axis=0)
        
    sink_data.to_csv(NetworkStatus_dir+'/4_CostAndFlow/2_SinkData_sourceinfo_'+ireg+'.csv',index=None)
    
    return flow_data,sink_data

def cost_cut(cap_data,pipe_data,sto_data,flow_data,sink_data,NetworkStatus_dir,ireg):
    source_cost = cap_data.loc[:,['Sector','Plant ID','CO2 Capature (Mt)','Capature Cost (USD)']]
    
    flow_data2 = pd.merge(flow_data,
                          pipe_data.loc[:,['Start','End','CO2 Transport (Mt)','Transport Cost (USD)']],
                          on=['Start','End'],how='left')
    
    flow_data2.columns = ['Start','End','Source','CO2 Transport (Mt)','CO2 Transport all(Mt)','Transport Cost (USD)']
    flow_data2['Transport Cost (USD)'] = flow_data2['Transport Cost (USD)']*flow_data2['CO2 Transport (Mt)']/flow_data2['CO2 Transport all(Mt)']
    flow_data2 = flow_data2.drop(['Start','End','CO2 Transport (Mt)','CO2 Transport all(Mt)'],axis=1)
    
    flow_data2 = flow_data2.groupby(['Source'],as_index=False).sum()
    
    source_cost = pd.merge(source_cost,flow_data2,left_on=['Plant ID'],right_on=['Source'],how='left')
    source_cost = source_cost.drop(['Source'],axis=1)
    sink_data2 = pd.merge(sink_data,
                          sto_data.loc[:,['Plant ID','CO2 Storage (Mt)','Storage Cost (USD)']],
                          on=['Plant ID'],how='left')
    
    sink_data2.columns = ['Plant ID','CO2 Storage (Mt)','Source','CO2 Storage (Mt) all','Storage Cost (USD)']
    sink_data2['Storage Cost (USD)'] = sink_data2['Storage Cost (USD)']*sink_data2['CO2 Storage (Mt)']/sink_data2['CO2 Storage (Mt) all']
    sink_data2 = sink_data2.drop(['Plant ID','CO2 Storage (Mt)','CO2 Storage (Mt) all'],axis=1)
    
    sink_data2 = sink_data2.groupby(['Source'],as_index=False).sum()
    
    source_cost = pd.merge(source_cost,sink_data2,left_on=['Plant ID'],right_on=['Source'],how='left')
    source_cost = source_cost.drop(['Source'],axis=1)
    
    source_cost['Cost per CO2 (USD/t)'] = source_cost.loc[:,'Capature Cost (USD)':].sum(axis=1)/source_cost.loc[:,'CO2 Capature (Mt)']/10**6
    
    source_cost.to_csv(NetworkStatus_dir+'/4_CostAndFlow/2_Cost_sourceinfo_'+ireg+'.csv',index=None)
    
    source_cost_rank = source_cost.loc[:,['Sector','Plant ID','Cost per CO2 (USD/t)']]
    source_cost_rank = source_cost_rank.sort_values(['Cost per CO2 (USD/t)']).reset_index(drop=True).reset_index(drop=False)
    source_cost_rank.rename(columns={'index':'Cost rank'},inplace=True)
    source_cost_rank.to_csv(NetworkStatus_dir+'/4_CostAndFlow/2_Cost_rank_'+ireg+'.csv',index=None)
    
    return source_cost

def faci_plot(df_ori,NetworkStatus_dir,ireg):
    mkdir(NetworkStatus_dir+'/figure')
    
    df_ori = df_ori.sort_values(['Cost per CO2 (USD/t)']).reset_index(drop=True)
    
    font2 = {'family' : 'Arial','weight' : 'normal','size' : 50};
    
    figsize=45,15
    fig, axis = plt.subplots(1,3,figsize=figsize)
    plt.subplots_adjust(wspace=0.4,hspace=0)
    
    sector = ['Power','IronAndSteel','Cement']
    color = ['#6fc011','#d915e0','#173d96']
    
    for i in range(3):
        df = df_ori.loc[df_ori['Sector']==sector[i],:].copy(deep=True)
        ax = axis[i]
        
        if df.shape[0]>0:
            df['Cumulative CO2 Capature (Mt)'] = df['CO2 Capature (Mt)'].cumsum(axis=0)
            ax.plot(df.loc[df['Sector']==sector[i],'Cumulative CO2 Capature (Mt)'],
                    df.loc[df['Sector']==sector[i],'Cost per CO2 (USD/t)'],
                    marker='o',markersize=15,color=color[i],
                    label='Coal power units',
                    linewidth=0)
        
            avg_y = sum(df.loc[df['Sector']==sector[i],'Cost per CO2 (USD/t)']*df.loc[df['Sector']==sector[i],'CO2 Capature (Mt)'])/sum(df.loc[df['Sector']==sector[i],'CO2 Capature (Mt)'])
            ax.text(0,avg_y,'%.2f'%avg_y+' USD/t')
            ax.axhline(y=avg_y,
                       xmin=0, xmax=df['Cumulative CO2 Capature (Mt)'].max(),
                       label='Average cost',
                       linestyle='--',linewidth=6,color='#36648B')
    
        ax.set_xlabel('Cumulative CO$_{2}$ Capature (Mt)',font2,labelpad=15);
        ax.set_ylabel('Total cost (USD/t CO$_{2}$)',font2,labelpad=15);
        #ax1.set_ylabel('Energy Efficiency (GJ/kt)',font2,labelpad=15);
        ax.set_title(sector[i],{'family':'Arial','weight':'normal','size':60},pad=15);
        
        ax.tick_params(which='both',length=20,labelsize=50);
        labels = ax.get_xticklabels() + ax.get_yticklabels() #+ ax1.get_yticklabels();
        [label.set_fontname("Arial") for label in labels];
        
        bwith = 3
        ax.spines['bottom'].set_linewidth(bwith);
        ax.spines['left'].set_linewidth(bwith);
        ax.spines['top'].set_linewidth(bwith);
        ax.spines['right'].set_linewidth(bwith);
    
    all_avg = avg_y = sum(df_ori.loc[:,'Cost per CO2 (USD/t)']*df_ori.loc[:,'CO2 Capature (Mt)'])/sum(df_ori.loc[:,'CO2 Capature (Mt)'])
    plt.suptitle(t='All average cost in '+ireg+': '+'%.2f'%all_avg+'USD/t',
                  fontdict=font2,y=0.98);
    
    plt.savefig(NetworkStatus_dir+'/figure/4_FacilityCost_'+ireg+'.jpg',dpi=50, bbox_inches='tight');
    
    return

def CostRank_main(ieng_sc,iend_sc,ior_sc,ireg,
                  SSM_dir,NetworkStatus_dir,
                  yr_beg,yr_end):
    
    cap_data = pd.read_csv(SSM_dir+'/capture_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    pipe_data = pd.read_csv(SSM_dir+'/transport_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    sto_data = pd.read_csv(SSM_dir+'/store_sec_'+ireg+'_'+str(yr_beg)+'_'+str(yr_end)+'.csv')
    
    cap_data = cap_data.loc[(cap_data['Year']==yr_end)&(cap_data['CO2 Capature (Mt)']>0),:].reset_index(drop=True)
    pipe_data = pipe_data.loc[(pipe_data['Year']==yr_end)&(pipe_data['CO2 Transport (Mt)']>0),:].reset_index(drop=True)
    sto_data = sto_data.loc[(sto_data['Year']==yr_end)&(sto_data['CO2 Storage (Mt)']>0),:].reset_index(drop=True)
    
    flow_cut,sink_cut = tracking_source(cap_data=cap_data,pipe_data=pipe_data,sto_data=sto_data,
                                        NetworkStatus_dir=NetworkStatus_dir,ireg=ireg)
    
    cost_source = cost_cut(cap_data=cap_data,pipe_data=pipe_data,
                           sto_data=sto_data,flow_data=flow_cut,sink_data=sink_cut,
                           NetworkStatus_dir=NetworkStatus_dir,ireg=ireg)
    
    faci_plot(df_ori=cost_source.loc[:,['Sector','CO2 Capature (Mt)','Cost per CO2 (USD/t)']],
              NetworkStatus_dir=NetworkStatus_dir,ireg=ireg)
    
    return

#%%
if __name__ == "__main__":
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Age'
    ireg='India'
    yr_beg=2020
    yr_end=2030
    
    SSM_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/4_SSM/'
    NetworkStatus_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/5_Network_'+str(yr_end)+'/'
    
    mkdir(SSM_dir)
    mkdir(NetworkStatus_dir)
    
    CostRank_main(ieng_sc,iend_sc,ior_sc,ireg,SSM_dir,NetworkStatus_dir,yr_beg,yr_end)