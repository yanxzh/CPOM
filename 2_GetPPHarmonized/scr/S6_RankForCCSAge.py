# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 21:19:03 2023

@author: 92978
"""

#%%
import pandas as pd
import numpy as np
import shutil
import os
from S0_GlobalENV import *

#%%
#新建文件夹
def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)    

#%%
mkdir('../output/6_CCSRank_Age/')

reg_list = [
            'China',
            'Other-Asia-and-Pacific',
            'Russia+Eastern-Europe',
            'Middle-East-and-Africa',
            'Canada+Latin-America',
            'Western-Europe',
            'India',
            'East-Asia',
            'United-States'
            ]

reg_list2 = [
            'China',
            'Other Asia and Pacific',
            'Russia+Eastern Europe',
            'Middle East and Africa',
            'Canada+Latin America',
            'Western Europe',
            'India',
            'East Asia',
            'United States'
            ]
        
for isec in ['Power','Cement','IronAndSteel']:
    for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
        rank = pd.read_csv('../output/3_AgeRank/'+isec+'_'+ifa+'.csv')
        
        for ireg in region_ls:
            
            reg1 = ireg
            reg2 = pd.Series(reg_list)[pd.Series(reg_list2)==ireg].values[0]
            
            pp = pd.read_pickle('../output/5_RegionPP/'+isec+'_'+ifa+'_'+ireg+'.pkl')
            pp = pd.merge(pp,rank.loc[:,['Plant ID','Age rank']],on='Plant ID',how='left')
            
            ####################删掉不在管网之内的点
            #引入管网数据
            pipline_net = pd.read_pickle('../../4_SSM/PreNet/PreNet_'+reg2+'/output/3_CandidatePipeline/Point2Point_'+reg1+'.pkl')
            max_distance = 200
            pipline_net['Distances (km)'] = pipline_net['Distances (km)'].astype(float)
            pipline_net = pipline_net.loc[pipline_net['Distances (km)']<=max_distance,:]
            ####################删掉不在管网之内的点
            
            #缩减表格
            #去掉管网之外的源汇，至少有一个点在管网里面
            pp = pp.loc[(np.isin(pp['Plant ID'],pipline_net['Start'])|(np.isin(pp['Plant ID'],pipline_net['End']))),:]
            pp = pp.reset_index(drop=True)
            ####################删掉不在管网之内的点
            
            df_out = pp.loc[:,['Plant ID','Age','Age rank']]
            df_out.to_csv('../output/6_CCSRank_Age/'+isec+'_'+ifa+'_'+ireg+'.csv',index=None,encoding='utf-8-sig')