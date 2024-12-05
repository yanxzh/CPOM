# -*- coding: utf-8 -*-
"""
Created on Mon Oct 30 13:28:30 2023

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
def distance_coun(a,b):
    all_lat1 = np.radians(a[1])
    all_lat2 = np.radians(b[1])
    all_lat = all_lat1-all_lat2
    
    all_lon1 = np.radians(a[0])
    all_lon2 = np.radians(b[0])
    all_lon = all_lon1-all_lon2
    
    a = np.sin(all_lat/2)**2 + np.cos(all_lat1)*np.cos(all_lat2)*np.sin(all_lon/2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
   
    df_dis = 6371.0*c
    
    return df_dis

def get_temp(df,co):
    for ico in range(co):
        df_ = pd.read_pickle('../temp/'+str(ico)+'.pkl')
        df = pd.concat([df,df_],axis=0)
        os.remove('../temp/'+str(ico)+'.pkl')
    df = df.reset_index(drop=True)
    
    return df

def G_multipro(net,pn,core,beg,end):
    G = nx.Graph()
    
    edge_data = pd.DataFrame(columns=['Lon_st','Lat_st','Lon_en','Lat_en','Distance (km)'])
    net_this = gpd.GeoDataFrame(geometry=gpd.GeoSeries(net.geoms[beg:end]))
    
    b = 0
    for line in net_this.loc[0,'geometry'].geoms:
        for edge in zip(line.coords[:-1], line.coords[1:]):
            print(b)
            b = b+1
            distance = distance_coun(*edge)
            
            if distance < 200:
                data_in = pd.DataFrame([edge[0][0],edge[0][1],edge[1][0],edge[1][1],distance]).T
                data_in.columns = ['Lon_st','Lat_st','Lon_en','Lat_en','Distance (km)']
                edge_data = pd.concat([edge_data,data_in],axis=0,ignore_index=True)
    
    edge_data.to_pickle('../temp/'+str(core)+'.pkl')
    
    return

def get_edge(net,pn):
    core_num = 56
    for icore in range(core_num):
        exec('p'+str(icore)+'=Process(target=G_multipro,\
              args=(net,pn,icore,'\
              +str(int(len(net.geoms)/core_num*(icore)))+\
              ','+str(int(len(net.geoms)/core_num*(icore+1)))+'))')
            
    # starting process 1&2
    for icore in range(core_num):
        exec('p'+str(icore)+'.start()')

    # wait until process 1&2 is finished
    for icore in range(core_num):
        exec('p'+str(icore)+'.join()')
        exec('del p'+str(icore))
    
    net_point = get_temp(df=pd.DataFrame(),co=core_num)
    net_point.to_pickle('../output/4_Network/EdgeAll_'+reg+'.pkl')
    
    return

def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)
        
#%%
if __name__ == '__main__':
    mkdir('../output/4_Network/')
    
    speedups.enable()
    
    network = gpd.read_file('../output/2_TriAndNet/0_NetWithlineAndPoint_'+reg+'.shp')
    all_point = pd.read_pickle('../output/3_ToPkl/AllPointName_'+reg+'.pkl')
    all_point = gpd.GeoDataFrame(all_point, geometry='geometry')
    
    row = network.loc[0,'geometry']
    
    get_edge(net=row,pn=all_point)
    
