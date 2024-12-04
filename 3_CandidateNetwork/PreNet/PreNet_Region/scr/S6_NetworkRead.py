# -*- coding: utf-8 -*-
"""
Created on Sat Jan 13 19:10:18 2024

@author: 92978
"""

#%%
import geopandas as gpd
import networkx as nx
import pandas as pd
import numpy as np
from shapely.geometry import Point, LineString
from shapely import speedups
from multiprocessing import Process
import os
from S0_GlobalENV import *

#%%
def get_point():
    isolated_points = gpd.read_file('../output/1_IsolatePoint/3_SelectedPoint_'+reg+'.shp')
    isolated_points = isolated_points.rename(columns={'ID':'Plant ID'})
    isolated_points = isolated_points.reset_index(drop=True)
    isolated_points = gpd.GeoDataFrame(isolated_points, geometry='geometry')

    return isolated_points

def get_temp(df,co):
    for ico in range(co):
        df_ = pd.read_pickle('../temp/'+str(ico)+'.pkl')
        df = pd.concat([df,df_],axis=0)
        os.remove('../temp/'+str(ico)+'.pkl')
    df = df.reset_index(drop=True)
    
    return df

def G_multiedge(net,po,core,beg,end):
    point = po.loc[beg:end,:].reset_index(drop=True)
    
    df_all = pd.DataFrame(columns=['Start','End','Distances (km)'])
    for ip in range(point.shape[0]):
    # for ip in range(0,1):
        po_id = point.loc[ip,'Plant ID']
        try:
            p_link = nx.single_source_dijkstra_path_length(G, source=po_id,weight='Distance (km)')
        except:
            print(po_id+' is not in network G!')
            continue

        p_link = pd.DataFrame(list(p_link.items()), columns=['End', 'Distances (km)'])
        p_link = p_link.loc[p_link['End'].str.contains('Si')|p_link['End'].str.contains('IS')|p_link['End'].str.contains('CM')|p_link['End'].str.contains('CPED')|p_link['End'].str.contains('WEPP'),:]
        p_link = p_link.loc[p_link['Distances (km)']<500,:]
        p_link.insert(loc=0,column='Start',value=po_id)
        
        df_all = pd.concat([df_all,p_link],axis=0)
        
    df_all.to_pickle('../temp/'+str(core)+'.pkl')
    
    print(core)
    
    return

def get_set(net,po):
    core_num = 10
    for icore in range(core_num):
        exec('p'+str(icore)+'=Process(target=G_multiedge,\
              args=(net,po,icore,'\
              +str(int(po.shape[0]/core_num*(icore)))+\
              ','+str(int(po.shape[0]/core_num*(icore+1)))+'))')
            
    # starting process 1&2
    for icore in range(core_num):
        exec('p'+str(icore)+'.start()')

    # wait until process 1&2 is finished
    for icore in range(core_num):
        exec('p'+str(icore)+'.join()')
        exec('del p'+str(icore))
    
    net_point = get_temp(df=pd.DataFrame(),co=core_num)
    net_point.to_pickle('../output/4_Network/Point2Point_'+reg+'.pkl')
    
    return

#%%
if __name__ == '__main__':
    G = nx.read_gexf('../output/4_Network/Final_net_'+reg+'.gexf')
    point = get_point()
    
    get_set(net=G,po=point)