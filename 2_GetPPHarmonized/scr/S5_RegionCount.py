# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 16:01:56 2024

@author: 92978
"""

#%%
import geopandas as gpd
import numpy as np
import shapely.geometry as sg
from shapely.geometry import Point, LineString, MultiPolygon
import pandas as pd
import os
from multiprocessing import Process
from S0_GlobalENV import *

#%%
def processing(df,net,icore,big,end):
    df = df[big:end].copy(deep=True)
    # df = list(df)
    filtered = df.within(net['geometry'].values[0].buffer(0))
    filtered = pd.Series(filtered)
    filtered.to_pickle('../temp/'+str(icore)+'.pkl')
    
    return

#合并出来的信息
def get_temp(df,co):
    for ico in range(co):
        df_ = pd.read_pickle('../temp/'+str(ico)+'.pkl')
        df = pd.concat([df,df_],axis=0)
        os.remove('../temp/'+str(ico)+'.pkl')
    df = df.reset_index(drop=True)
    
    return df
    
def mul_main(df,net):
    core_num = 20
    for icore in range(core_num):
        print(icore,flush=True)
        # processing(df=df,net=net,icore=icore,
        #             big=int(df.shape[0]/core_num*(icore)),end=int(df.shape[0]/core_num*(icore+1)))
        exec('p'+str(icore)+'=Process(target=processing,\
              args=(df,net,icore,'\
              +str(int(df.shape[0]/core_num*(icore)))+\
              ','+str(int(df.shape[0]/core_num*(icore+1)))+'))')
    
    # starting process 1&2
    for icore in range(core_num):
        exec('p'+str(icore)+'.start()')

    # wait until process 1&2 is finished
    for icore in range(core_num):
        exec('p'+str(icore)+'.join()')
        exec('del p'+str(icore))
    
    pipline = get_temp(df=pd.Series(),co=core_num)
    
    return pipline

#%%
if __name__ == '__main__':
    mkdir('../output/5_RegionPP/')
    
    world_map = gpd.read_file('../input/Region_map/Region_map.shp',crs='WGS84')
    
    data_get = pd.DataFrame()
    for isec in ['Power','Cement','IronAndSteel']:
        pp = pd.read_pickle('../output/1_PlantLevelPP/'+isec+'.pkl')
        
        faciliy = fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']
        
        for ifa in faciliy:
            print(isec+'_'+ifa,flush=True)
            gid_data = pd.read_pickle('../output/2_PP_cut/'+isec+'_'+ifa+'.pkl')
            gid_geo = gid_data.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
            gid_geo = gpd.GeoSeries(gid_geo, crs='WGS84')
            
            emis_all = gid_data['CO2 Emissions'].sum()
            cap_all = gid_data['Capacity'].sum()
            num_all = gid_data.shape[0]
            
            for ire in [
              'India',
              'China',
              'United States',
              'Russia+Eastern Europe',
              'Middle East and Africa',
              'Canada+Latin America',
              'Western Europe',
              'East Asia',
              'Other Asia and Pacific',
              ]:
            # for ire in ['India']:
            # for ire in ['China']:
                print(ire,flush=True)
                re_gdf = world_map.loc[world_map['Region']==ire,:]
                
                pp_filtered = mul_main(df=gid_geo,net=re_gdf)
                # pp_filtered = gid_geo.within(re_gdf['geometry'].values[0].buffer(0))
                re_gid = gid_data.loc[pp_filtered.values,:].reset_index(drop=True)
                
                re_gid['Country'] = ire
                re_gid.to_pickle('../output/5_RegionPP/'+isec+'_'+ifa+'_'+ire+'.pkl')
            
                emis_re = re_gid['CO2 Emissions'].sum()/emis_all
                cap_re = re_gid['Capacity'].sum()/cap_all
                num_re = re_gid.shape[0]/num_all
                
                df_in = pd.DataFrame([ire,isec,ifa,emis_re,cap_re,num_re],
                                      index=['Region','Sector','Facility Type','Emission ratio','Capacity ratio','Number ratio'])
                
                data_get = pd.concat([data_get,df_in.T],axis=0)
                
            df_in = pd.DataFrame(['World',isec,emis_all,cap_all,num_all],
                                  index=['Region','Sector','Emission ratio','Capacity ratio','Number ratio'])
            data_get = pd.concat([data_get,df_in.T],axis=0)
        
    data_get.to_csv('../output/5_RegionPP/CountAll.csv',index=None)
