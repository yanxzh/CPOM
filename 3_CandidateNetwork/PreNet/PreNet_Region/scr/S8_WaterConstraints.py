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
import random
import xarray as xr
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

#%%
def find_nearest(array, value):
    idx = (abs(array - value)).argmin()
    return array[idx]

def get_square(mask):
    lat = mask['lat']
    lon = mask['lon']

    lat_rad = np.deg2rad(lat)
    dlat = np.deg2rad(0.5)
    dlon = np.deg2rad(0.5)

    R = 6371000

    lat_top = lat_rad + dlat / 2
    lat_bottom = lat_rad - dlat / 2

    area_per_lat = (R**2) * dlon * (np.sin(lat_top) - np.sin(lat_bottom))  # shape: (lat,)

    area_2d = np.tile(area_per_lat.values[:, np.newaxis], (1, len(lon)))
    mask['cell_area'] = (('lat', 'lon'), area_2d)

    return mask

def coms_data(df):
    # print(df.columns)
    df_loc = df.copy(deep=True)
    mask = xr.open_dataset('/public/work/yanxizhe/GeoConstrain/GlobalWaterMap/Demand/output/AnnualDemand_0.5deg.nc')
    mask = get_square(mask)

    mask['total_gross_demand'] = (mask['total_gross_demand'] / 20)* mask['cell_area']*10**(-6)

    values = []
    for _, row in df_loc.iterrows():
        val = mask['total_gross_demand'].sel(
            lon=row['Longitude'],
            lat=row['Latitude'],
            method='nearest'
        ).item()
        values.append(val)

    lat_array = mask['lat'].values
    lon_array = mask['lon'].values

    df_loc['WaterOriginDemand'] = values
    df_loc['Pool_Lat'] = df_loc['Latitude'].apply(lambda x: find_nearest(lat_array, x))
    df_loc['Pool_Lon'] = df_loc['Longitude'].apply(lambda x: find_nearest(lon_array, x))
    
    df_loc['Pool_ID'] = df_loc.groupby(['Pool_Lat', 'Pool_Lon'], sort=False).ngroup()
    df_loc['Water Unit'] = '10^6 m3'

    mask.close()

    return df_loc

def res_data(df_loc):
    # print(df.columns)
    mask = xr.open_dataset('/public/work/yanxizhe/GeoConstrain/GlobalWaterMap/RunOff_ERA-5/output/AnnualRunoff_0.5deg.nc')
    mask = mask.rename({'longitude': 'lon','latitude': 'lat'})
    mask = get_square(mask)

    mask['annual_runoff'] = mask['annual_runoff']*mask['cell_area']*10**(-6)
    print(np.nanpercentile(mask['annual_runoff'].values,[20,50,70,95,100]))
    print(np.nanmax(mask['annual_runoff'].values))

    values = []
    for _, row in df_loc.iterrows():
        val = mask['annual_runoff'].sel(
            lon=row['Longitude'],
            lat=row['Latitude'],
            method='nearest'
        ).item()
        values.append(val)

    df_loc['WaterResource'] = values

    mask.close()

    return df_loc

#%%
def WaterConstraints_Main(sourcesink):
    ss_mask = coms_data(df=sourcesink)
    ss_mask = res_data(df_loc=ss_mask)
    ss_mask = gpd.GeoDataFrame(ss_mask, geometry='geometry',crs='EPSG:4326')

    return ss_mask

#%%
if __name__ == "__main__":
    reg = 'India'
    df = gpd.read_file('../output/1_IsolatePoint/0_point_'+reg+'.shp',crs='EPSG:4326')
    df2 = WaterConstraints_Main(sourcesink=df.copy(deep=True))
