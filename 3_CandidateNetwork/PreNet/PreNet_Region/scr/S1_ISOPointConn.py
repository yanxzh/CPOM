# -*- coding: utf-8 -*-
"""
Created on Sat Jan 13 16:22:49 2024

@author: 92978
"""

#%%
import geopandas as gpd
import networkx as nx
import pandas as pd
import numpy as np
from shapely.geometry import Point, LineString, GeometryCollection
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
from shapely.geometry import MultiLineString
import random
from S0_GlobalENV import *
from shapely.geometry import shape, mapping
from S7_ExcludeMask import ExcludeMask_Main
from S8_WaterConstraints import WaterConstraints_Main

#%%
def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)
        
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

def get_temp(df,df_iso,co):
    for ico in range(co):
        df_ = pd.read_pickle('../temp/'+str(ico)+'.pkl')
        df = pd.concat([df,df_],axis=0)
        os.remove('../temp/'+str(ico)+'.pkl')
        
        df_iso_ = pd.read_pickle('../temp/iso_'+str(ico)+'.pkl')
        df_iso = pd.concat([df_iso,df_iso_],axis=0)
        os.remove('../temp/iso_'+str(ico)+'.pkl')
        
    df = df.reset_index(drop=True)
    df_iso = df_iso.reset_index(drop=True)
    
    return df,df_iso

def clear_point(rn):
    print('Creat Point SHP',flush=True)
    
    sink = gpd.read_file('../../Regional_Input/Region_Sink/Potential_sink_'+reg+'.shp',crs='EPSG:4326')
    sink.insert(column='Capacity',loc=sink.shape[1],value=np.nan)
    sink.set_crs(epsg=4326, inplace=True)
    
    fa_dict = pd.DataFrame((['Power','Coal'],['Power','Gas'],['Power','Oil'],
                            ['IronAndSteel','Iron'],['Cement','Clinker']),columns=['Sector','Facility Type'])
    
    source = pd.DataFrame()
    for isec in ['Power','Cement','IronAndSteel']:
        for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
            df = pd.read_pickle('../../Regional_Input/Region_Source/'+isec+'_'+ifa+'_'+reg+'.pkl')
            df = df.loc[:,['Sector','Plant ID','Longitude','Latitude','CO2 Emissions','Capacity']]
            source = pd.concat([source,df],axis=0)
    
    source.reset_index(drop=True,inplace=True)
    source.columns = ['Sector','Plant ID','Longitude', 'Latitude', 'CO2','Capacity']
    source = source.reset_index(drop=True)
    source.loc[:,['Longitude','Latitude','CO2','Capacity']] = \
        source.loc[:,['Longitude','Latitude','CO2','Capacity']].astype(float)
    
    source['Capacity'] = -9
    
    source = source.reset_index(drop=True).reset_index(drop=False)
    source.rename(columns={'Plant ID':'ID'},inplace=True)
    source = gpd.GeoDataFrame(source, geometry=gpd.points_from_xy(source['Longitude'], source['Latitude']),crs='EPSG:4326')
    
    sink = sink.reset_index(drop=True).reset_index(drop=False)
    sink['ID'] = 'Si_'+sink['index'].astype(str)

    sink.insert(column='Type',value='Sink',loc=0)
    sink.insert(column='Sector',value='Sink',loc=0)
    source = gpd.GeoDataFrame(source, geometry=gpd.points_from_xy(source['Longitude'], source['Latitude']))
    source.insert(column='Type',value='Source',loc=0)
    source.insert(column='DSA',loc=source.shape[1],value=np.nan)
    
    all_data = pd.concat([sink.loc[:,['ID','Type','Sector','DSA','Longitude','Latitude','CO2','Capacity','geometry']],
                          source.loc[:,['ID','Type','Sector','DSA','Longitude','Latitude','CO2','Capacity','geometry']]],axis=0)
    all_data = all_data.reset_index(drop=True)
    all_data = gpd.GeoDataFrame(all_data, geometry=all_data['geometry'],crs='EPSG:4326')
    
    all_data = ExcludeMask_Main(sourcesink=all_data.copy(deep=True))
    all_data = WaterConstraints_Main(sourcesink=all_data.copy(deep=True))

    all_data.to_file('../output/1_IsolatePoint/0_point_'+reg+'.shp', driver='ESRI Shapefile',encoding='utf-8')
    
    return

def point_connect(po,net,core,nanf,big,end):
    print(core,flush=True)
    po = po.loc[big:end,:]
    
    line_list = []
    iso_list = []
    
    for idx, point in po.iterrows():
        # print(idx)
        nearest_road = net.distance(po.loc[idx,'geometry']).idxmin()
        nearest_line = net.loc[nearest_road, 'geometry']
        nearest_point = nearest_points(nearest_line, point.geometry)[0]
        
        point_a = (nearest_point.x, nearest_point.y)
        point_b = (point.geometry.x, point.geometry.y)
        distance_km = distance_coun(point_a, point_b)

        if distance_km <= 100:
            extended_line = LineString([nearest_point, point.geometry])

            direction_vector = Point(point.geometry.x - nearest_point.x, point.geometry.y - nearest_point.y)

            dx, dy = direction_vector.xy
            nearest_point2 = Point(nearest_point.x - 0.001 * dx[0], nearest_point.y -0.001 * dy[0])
            extended_line2 = LineString([nearest_point2, point.geometry])
    
            new_line = nanf.copy()
            new_line.loc[:, 'geometry'] = extended_line2
            line_list.append(new_line)
            
        else:
            iso_list.append(point)
    
    df = pd.concat(line_list, ignore_index=True)
    iso_point = pd.DataFrame(iso_list,columns=['geometry'])

    df.to_pickle('../temp/'+str(core)+'.pkl')
    iso_point.to_pickle('../temp/iso_'+str(core)+'.pkl')
    
    return
    
def point_connect_main(po,net):
    print('Connect',flush=True)
    nanf = pd.DataFrame(np.repeat([None],net.shape[1],axis=0)).T
    nanf.columns = net.columns
    net = net.reset_index(drop=True)
    
    # point_connect(po,net,0,nanf,0,int(po.shape[0]/56))
    
    core_num = 10
    for icore in range(core_num):
        exec('p'+str(icore)+'=Process(target=point_connect,\
              args=(po,net,icore,nanf,'\
              +str(int(po.shape[0]/core_num*(icore)))+\
              ','+str(int(po.shape[0]/core_num*(icore+1)))+'))')
            
    # starting process 1&2
    for icore in range(core_num):
        exec('p'+str(icore)+'.start()')

    # wait until process 1&2 is finished
    for icore in range(core_num):
        exec('p'+str(icore)+'.join()')
        exec('del p'+str(icore))
    
    pc,iso_p = get_temp(df=pd.DataFrame(),df_iso=pd.DataFrame(),co=core_num)
    net = pd.concat([net,pc],axis=0)
    
    iso_p = gpd.GeoDataFrame(iso_p, geometry='geometry',crs='EPSG:4326')
    
    net.to_file('../output/1_IsolatePoint/1_net_'+reg+'.shp', driver='ESRI Shapefile',encoding='utf-8')
    iso_p.to_file('../output/1_IsolatePoint/1_IsolatePoint_'+reg+'.shp', driver='ESRI Shapefile',encoding='utf-8')
    
    return

def round_coords(geom, precision=5):
    if geom.is_empty:
        return geom
    
    def round_coords_recursive(coords):
        if isinstance(coords, (list, tuple)):
            return type(coords)(round_coords_recursive(coord) for coord in coords)
        else:
            return round(coords, precision)
    
    geom_mapping = mapping(geom)
    rounded_coords = round_coords_recursive(geom_mapping['coordinates'])
    geom_mapping['coordinates'] = rounded_coords
    
    return shape(geom_mapping)
    
def network_all():
    road = gpd.read_file('../../Regional_Input/Region_Road/PrimaryRoad_'+reg+'.shp',crs='EPSG:4326')
    pipe = gpd.read_file('../../Regional_Input/Region_GasPipe/GasPipe_'+reg+'.shp',crs='EPSG:4326')
    
    road_0 = road.loc[[0],:]
    road_0.loc[0,:] = np.nan
    pipe_all = pd.DataFrame(np.repeat(road_0.values,pipe.shape[0],axis=0),columns=road_0.columns)
    pipe_all['geometry'] = pipe['geometry'].values
    
    df_all = pd.concat([road,pipe_all],axis=0)
    df_all.reset_index(drop=True,inplace=True)
    df_all = gpd.GeoDataFrame(df_all,geometry='geometry',crs='EPSG:4326')
    
    df_all2 = df_all.copy(deep=True)
    df_all2.reset_index(drop=True,inplace=True)

    ls = []
    while df_all2.shape[0] > 0:
        
        a = np.min(df_all2.index)
        print(a,flush=True)
        
        geo_this = df_all2.loc[a, 'geometry']
        intersect = df_all2.intersects(geo_this)
        ls_a = df_all2.index[intersect].tolist()
        
        processed_ls_a = [a]
        
        while True:
            other_nonpro = list(set(ls_a) - set(processed_ls_a))
            if len(other_nonpro) == 0:
                df_all2 = df_all2.loc[np.isin(df_all2.index,ls_a)==0,:]
                ls.append(ls_a)
                break
            
            for idx in other_nonpro:
                intersect = df_all2.loc[np.isin(df_all2.index,ls_a)==0,:].intersects(df_all2.loc[idx, 'geometry'])
                new_intersections = df_all2.loc[np.isin(df_all2.index,ls_a)==0,:].index[intersect].tolist()
                ls_a = ls_a+new_intersections
                
                processed_ls_a = processed_ls_a+[idx]
        
        if df_all2.shape[0] == 0:
            break
    
    geo_all = []
    for ils in ls:
        geo_all.append(unary_union(df_all.loc[np.isin(df_all.index,ils),'geometry']))
        
    df_all = gpd.GeoDataFrame(geometry=geo_all, crs='EPSG:4326')

    df_all['length'] = df_all.length
    a = df_all['length']>2
    b = df_all['length']<=2
    
    b = df_all.loc[b,:]
    a = df_all.loc[a,:]
    
    try:
        fig, ax = plt.subplots(figsize=(50, 40)); 
        a.plot(ax=ax)
        plt.savefig('../figure/pass.jpg',dpi=100,bbox_inches='tight',format='jpg')
    except:
        pass
    
    try:
        fig, ax = plt.subplots(figsize=(50, 40)); 
        b.plot(ax=ax)
        plt.savefig('../figure/nonpass.jpg',dpi=100,bbox_inches='tight',format='jpg')
    except:
        pass
    
    a = a.reset_index(drop=True)
    
    return a

def clean_and_merge_lines(road_network: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    unioned = unary_union(road_network.geometry.values)
    merged = linemerge(unioned)

    lines = []

    def unpack(geom):
        if isinstance(geom, LineString):
            lines.append(geom)
        elif isinstance(geom, MultiLineString):
            lines.extend(geom.geoms)
        elif isinstance(geom, GeometryCollection):
            for g in geom.geoms:
                unpack(g)

    unpack(merged)

    return gpd.GeoDataFrame(geometry=lines, crs='EPSG:4326')

#%%
if __name__ == '__main__':
    mkdir('../output/1_IsolatePoint/')
    
    road_network = network_all()
    clear_point(rn=road_network)

    isolated_points = gpd.read_file('../output/1_IsolatePoint/0_point_'+reg+'.shp',crs='EPSG:4326')
    isolated_points = isolated_points.to_crs('EPSG:4326')
    road_network = road_network.to_crs('EPSG:4326')
    point_connect_main(po=isolated_points,net=road_network)
        
    road_network = gpd.read_file('../output/1_IsolatePoint/1_net_'+reg+'.shp',crs='EPSG:4326')
    processed_gdf = clean_and_merge_lines(road_network)
    
    processed_gdf.to_file('../output/1_IsolatePoint/2_NetWithlineAndPoint_'+reg+'.shp', driver='ESRI Shapefile',encoding='utf-8')
    isolated_points.to_file('../output/1_IsolatePoint/3_SelectedPoint_'+reg+'.shp', driver='ESRI Shapefile',encoding='utf-8')