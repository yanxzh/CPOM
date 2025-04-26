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
import xarray as xr
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

#%%
def mask_urban(df):
    # print(df.columns)
    df_loc = df.copy(deep=True)
    mask = xr.open_dataset('/public/work/yanxizhe/GeoConstrain/OtherData/LandUse/output/UrbanGrid_mask.nc')

    values = []
    for _, row in df_loc.iterrows():
        val = mask['urban_mask'].sel(
            lon=row['Longitude'],
            lat=row['Latitude'],
            method='nearest'
        ).item()
        values.append(val)
    
    # 4. 加入新列表示是否是城市土地利用类型
    df_loc['UrbanCode'] = values
    
    mask.close()

    return df_loc

def mask_protected(df):
    # print(df.columns)
    mask = xr.open_dataset('/public/work/yanxizhe/GeoConstrain/OtherData/ProtectedAreas/output/protected_areas_masked_by_land.nc')

    values = []
    for _, row in df.iterrows():
        val = mask['protected_area_mask_masked'].sel(
            lon=row['Longitude'],
            lat=row['Latitude'],
            method='nearest'
        ).item()
        values.append(val)

    df['ProtectedCode'] = values
    
    mask.close()

    return df

def mask_popden(df):
    # print(df.columns)
    mask = xr.open_dataset('/public/work/yanxizhe/GeoConstrain/OtherData/POP/output/POP90th_RegionVersion.nc')

    values = []
    for _, row in df.iterrows():
        val = mask['POP90th_mask'].sel(
            lon=row['Longitude'],
            lat=row['Latitude'],
            method='nearest'
        ).item()
        values.append(val)

    df['POP90thCode'] = values
    
    mask.close()

    return df

#%%
def ExcludeMask_Main(sourcesink):

    ss_mask = mask_urban(df=sourcesink)
    ss_mask = mask_protected(df=ss_mask)
    ss_mask = mask_popden(df=ss_mask)

    ss_mask = ss_mask.loc[(ss_mask['UrbanCode']==0)&\
                          (ss_mask['POP90thCode']==0)&\
                          (ss_mask['ProtectedCode']==0),:].copy(deep=True)
    
    ss_mask.drop(['UrbanCode','POP90thCode','ProtectedCode'],axis=1,inplace=True)
    ss_mask = gpd.GeoDataFrame(ss_mask, geometry='geometry',crs='EPSG:4326')

    return ss_mask

#%%
if __name__ == "__main__":
    reg = 'India'
    df = gpd.read_file('../output/1_IsolatePoint/0_point_'+reg+'.shp',crs='EPSG:4326')
    df2 = ExcludeMask_Main(sourcesink=df.copy(deep=True))