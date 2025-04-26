# -*- coding: utf-8 -*-
"""
Created on Sat Apr  6 21:01:29 2024

@author: 92978
"""

#%%
import pandas as pd
import numpy as np
from S1_Global_ENV import *
import time
import os

#%%
#测试使用
def StorageChange_main(ieng_sc,iend_sc,ior_sc,ireg,
                       CCSInstall_dir,SSM_dir,NetworkStatus_dir,
                       yr_beg,yr_end):
    
    GID_df = pd.read_csv(CCSInstall_dir + ireg + '_GIDAll_' + str(yr_beg) + '_' + str(yr_end) + '.csv', encoding='utf-8-sig')
    cap_df = GID_df.loc[GID_df['CO2 Eta (%)'] == 90, ['Plant ID', 'CO2 Emissions']]
    cap_df['Capture (Mt)'] = cap_df['CO2 Emissions'] * 9 / 10**6
    cap_df = cap_df.loc[:, ['Plant ID', 'Capture (Mt)']].groupby(['Plant ID'], as_index=False).sum()
    
    cap_data = pd.read_csv(SSM_dir + '/capture_sec_' + ireg + '_' + str(yr_beg) + '_' + str(yr_end) + '.csv')
    sink_data = pd.read_csv(NetworkStatus_dir + '/4_CostAndFlow/2_SinkData_sourceinfo_' + ireg + '.csv')
    sink_data.columns = ['Sink', 'CO2 Storage (Mt)', 'Plant ID']
    sink_data = pd.merge(sink_data, cap_data.loc[:, ['Plant ID', 'CO2 Capature (Mt)']], on='Plant ID', how='left')
    
    sink_data['Ratio'] = sink_data['CO2 Storage (Mt)'] / sink_data['CO2 Capature (Mt)']
    sink_data.drop(['CO2 Storage (Mt)', 'CO2 Capature (Mt)'], axis=1, inplace=True)
    
    sink_data = pd.merge(sink_data, cap_df.loc[:, ['Plant ID', 'Capture (Mt)']], on='Plant ID', how='left')
    sink_data['Capture (Mt)'] = sink_data['Capture (Mt)'] * sink_data['Ratio']
    sink_data = sink_data.loc[:, ['Sink', 'Capture (Mt)']].groupby(['Sink'], as_index=False).sum()
    
    sink_data.to_csv(SSM_dir + ireg + '_StorageReduce_' + str(yr_beg) + '_' + str(yr_end) + '.csv', encoding='utf-8-sig', index=None)
    
    return

#%%
if __name__ == '__main__':
    ieng_sc='Neut'
    iend_sc='BAU'
    ior_sc='Cost'
    ireg='India'
    
    yr_beg=2040
    yr_end=2050
    
    CCSInstall_dir=OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/6_CCSInstall/'
    SSM_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/4_SSM/'
    NetworkStatus_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/5_Network_'+str(yr_end)+'/'
    
    mkdir(CCSInstall_dir)
    mkdir(NetworkStatus_dir)
    
    StorageChange_main(ieng_sc,iend_sc,ior_sc,ireg,
                       CCSInstall_dir,SSM_dir,NetworkStatus_dir,
                       yr_beg,yr_end)