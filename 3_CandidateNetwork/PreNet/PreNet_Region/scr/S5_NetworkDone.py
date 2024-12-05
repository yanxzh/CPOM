# -*- coding: utf-8 -*-
"""
Created on Sat Jan 13 10:49:39 2024

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
if __name__ == '__main__':
    all_point = pd.read_pickle('../output/3_ToPkl/AllPointName_'+reg+'.pkl')
    isolated_points = gpd.read_file('../output/1_IsolatePoint/3_SelectedPoint_'+reg+'.shp')
    isolated_points = isolated_points.rename(columns={'ID':'Plant ID'})
    
    all_point = gpd.GeoDataFrame(all_point, geometry='geometry')
    isolated_points = gpd.GeoDataFrame(isolated_points, geometry='geometry')
    
    all_edge = pd.read_pickle('../output/4_Network/EdgeAll_'+reg+'.pkl')
    
    all_point = all_point.drop_duplicates(['Lon','Lat'])
    all_edge = all_edge.drop_duplicates(['Lon_st', 'Lat_st', 'Lon_en', 'Lat_en'])
    
    all_edge.loc[:,['Lon_st', 'Lat_st', 'Lon_en', 'Lat_en']] = all_edge.loc[:,['Lon_st', 'Lat_st', 'Lon_en', 'Lat_en']].astype(float).round(4)
    isolated_points.loc[:,['Longitude', 'Latitude']] = isolated_points.loc[:,['Longitude', 'Latitude']].astype(float).round(4)
    all_point.loc[:,['Lon', 'Lat']] = all_point.loc[:,['Lon', 'Lat']].astype(float).round(4)
    
    all_edge = pd.merge(all_edge,isolated_points.loc[:,['Plant ID','Longitude', 'Latitude']],
                        left_on=['Lon_st', 'Lat_st'],right_on=['Longitude', 'Latitude'],how='left')
    all_edge.rename(columns={'Plant ID':'Start'},inplace=True)
    all_edge = pd.merge(all_edge,isolated_points.loc[:,['Plant ID','Longitude', 'Latitude']],
                        left_on=['Lon_en', 'Lat_en'],right_on=['Longitude', 'Latitude'],how='left')
    all_edge.rename(columns={'Plant ID':'End'},inplace=True)
    all_edge = all_edge.loc[:,['Start','Lon_st', 'Lat_st', 'End','Lon_en', 'Lat_en','Distance (km)']]
    

    #%%
    change = all_edge.loc[pd.isnull(all_edge['Start']),:].reset_index(drop=True)
    change.drop(['Start'],axis=1,inplace=True)
    change = pd.merge(change,all_point.loc[:,['Plant ID', 'Lon', 'Lat']],
                      left_on=['Lon_st', 'Lat_st'],right_on=['Lon', 'Lat'],how='left')
    change.rename(columns={'Plant ID':'Start'},inplace=True)
    change = change.loc[:,['Start','Lon_st', 'Lat_st', 'End','Lon_en', 'Lat_en','Distance (km)']]
    
    all_edge = pd.concat([all_edge.loc[pd.isnull(all_edge['Start'])==0,:],change],axis=0)
    
    change = all_edge.loc[pd.isnull(all_edge['End']),:].reset_index(drop=True)
    change.drop(['End'],axis=1,inplace=True)
    change = pd.merge(change,all_point.loc[:,['Plant ID', 'Lon', 'Lat']],
                      left_on=['Lon_en', 'Lat_en'],right_on=['Lon', 'Lat'],how='left')
    change.rename(columns={'Plant ID':'End'},inplace=True)
    change = change.loc[:,['Start','Lon_st', 'Lat_st', 'End','Lon_en', 'Lat_en','Distance (km)']]
    
    all_edge = pd.concat([all_edge.loc[pd.isnull(all_edge['End'])==0,:],change],axis=0)
    all_edge = all_edge.reset_index(drop=True)
    
    G = nx.from_pandas_edgelist(all_edge, 'Start', 'End', edge_attr='Distance (km)')
    nx.write_gexf(G,'../output/4_Network/Final_net_'+reg+'.gexf')