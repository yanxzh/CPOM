# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 20:44:09 2024

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
def net_data(net,core,beg,end):
    net_point = pd.DataFrame(columns=['Plant ID','Lon','Lat','geometry'])
    a = 0
    
    net_this = gpd.GeoDataFrame(geometry=gpd.GeoSeries(net.geoms[beg:end]))
    
    for line in net_this.loc[0,'geometry'].geoms:
        for edge in zip(line.coords[:-1], line.coords[1:]):
            # Extract coordinates of the endpoints
            start_point = edge[0]
            end_point = edge[1]
            
            beg_name = 'm_'+str(core)+'_'+str(a)
            a = a+1
            data_beg = pd.DataFrame([beg_name,start_point[0],start_point[1],Point(start_point)]).T
            data_beg.columns = ['Plant ID','Lon','Lat','geometry']
            
            end_name = 'm_'+str(core)+'_'+str(a)
            a = a+1
            data_end = pd.DataFrame([end_name,end_point[0],end_point[1],Point(end_point)]).T
            data_end.columns = ['Plant ID','Lon','Lat','geometry']
            
            net_point = pd.concat([net_point,data_beg,data_end])
        print(a)
        
    net_point.to_pickle('../temp/'+str(core)+'.pkl')
        
    return

def get_point(net):

    core_num = 56
    for icore in range(core_num):
        exec('p'+str(icore)+'=Process(target=net_data,\
              args=(net,icore,'\
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
    
    return net_point

def get_temp(df,co):
    for ico in range(co):
        df_ = pd.read_pickle('../temp/'+str(ico)+'.pkl')
        df = pd.concat([df,df_],axis=0)
        os.remove('../temp/'+str(ico)+'.pkl')
    df = df.reset_index(drop=True)
    
    return df

def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)
        
#%%
if __name__ == '__main__':
    mkdir('../output/3_ToPkl/')
    
    speedups.enable()
    
    network = gpd.read_file('../output/2_TriAndNet/0_NetWithlineAndPoint_'+reg+'.shp')
    row = network.loc[0,'geometry']

    all_data = get_point(net=row)
    all_data.to_pickle('../output/3_ToPkl/AllPointName_'+reg+'.pkl')