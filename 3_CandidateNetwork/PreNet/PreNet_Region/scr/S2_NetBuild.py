# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 19:32:56 2024

@author: 92978
"""

#%%
import geopandas as gpd
import networkx as nx
import pandas as pd
import numpy as np
from shapely.geometry import Point, LineString,MultiLineString
from shapely.ops import nearest_points
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.colors as col
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
plt.rcParams['axes.unicode_minus']=False
from shapely.ops import unary_union,split
from multiprocessing import Process
import os
from shapely.ops import linemerge, unary_union
import random
from S0_GlobalENV import *
from scipy.spatial import Delaunay

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

def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)
        
#%%
if __name__ == '__main__':
    mkdir('../output/2_TriAndNet/')
    
    now_net = gpd.read_file('../output/1_IsolatePoint/2_NetWithlineAndPoint_'+reg+'.shp',crs='EPSG:4326')
    now_iso = gpd.read_file('../output/1_IsolatePoint/1_IsolatePoint_'+reg+'.shp',crs='EPSG:4326')
    
    G = nx.Graph()
    
    for line in now_net.loc[0,'geometry'].geoms:
        for edge in zip(line.coords[:-1], line.coords[1:]):
            # Extract coordinates of the endpoints
            start_point = edge[0]
            end_point = edge[1]
            G.add_edge(start_point, start_point)
    
    points = []
    for ishp in range(now_iso.shape[0]):
        points.append(now_iso.loc[ishp,'geometry'].coords[0])
    points = np.array(points)
    
    try:
        tri = Delaunay(points)

        edges = []
        for simplex in tri.simplices:
            for i in range(3):
                edge = (simplex[i], simplex[(i + 1) % 3])
                
                point_a = points[edge[0]]
                point_b = points[edge[1]]
                distance_km = distance_coun(point_a, point_b)

                if distance_km <= 800:
                    edges.append(LineString([point_a,point_b]))

        edge_gdf = gpd.GeoDataFrame(geometry=edges)
    
    except:
        point_a = points[0]
        point_b = points[1]
        distance_km = distance_coun(point_a, point_b)

        if distance_km <= 800:
            edges = LineString([Point(point_a),Point(point_b)])
            edge_gdf = gpd.GeoDataFrame(geometry=[edges])
        else:
            edge_gdf = gpd.GeoDataFrame(geometry=[])
    
    
    #%%
    all_geo = pd.concat([now_net['geometry'],edge_gdf['geometry']],axis=0)
    all_geo = all_geo.explode()
            
    multilines = MultiLineString(all_geo.geometry.values)

    merged_lines = linemerge(multilines)
    
    if not merged_lines.is_simple:
        merged_lines = unary_union(merged_lines)
    print('Out')
    
    processed_gdf = gpd.GeoDataFrame(geometry=[merged_lines],crs='EPSG:4326')
    
    fig, ax = plt.subplots(figsize=(50, 40));
    processed_gdf.plot(ax=ax,edgecolor='#53868B',linewidth=3)
     
    ax.set_xticks([])
    ax.set_yticks([])
    
    ax.spines['bottom'].set_linewidth(bwith);
    ax.spines['left'].set_linewidth(bwith);
    ax.spines['top'].set_linewidth(bwith);
    ax.spines['right'].set_linewidth(bwith);
    
    plt.savefig('../figure/AndNet_'+reg+'.jpg',dpi=100,bbox_inches='tight',format='jpg')
    processed_gdf.to_file('../output/2_TriAndNet/0_NetWithlineAndPoint_'+reg+'.shp', driver='ESRI Shapefile',encoding='utf-8')