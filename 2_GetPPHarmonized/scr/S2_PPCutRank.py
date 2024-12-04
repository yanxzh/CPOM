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

#%%
#新建文件夹
def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)    

#%%
mkdir('../output/2_PP_cut/')
mkdir('../output/3_AgeRank/')

np.random.seed(2)
        
for isec in ['Power','Cement','IronAndSteel']:
    pp = pd.read_pickle('../output/1_PlantLevelPP/'+isec+'.pkl')
    
    faciliy = pp['Facility Type'].drop_duplicates()
    
    for ifa in faciliy:
        df_out = pp.loc[pp['Facility Type']==ifa,:].reset_index(drop=True)
        df_out.to_pickle('../output/2_PP_cut/'+isec+'_'+ifa+'.pkl')
        df_out.to_csv('../output/2_PP_cut/'+isec+'_'+ifa+'.csv',index=None,encoding='utf-8-sig')
              
        df_out['Random_age'] = np.random.randint(0,high=df_out.shape[0],
                                                 size=df_out.shape[0],dtype='l')
        df_out = df_out.sort_values(['Age','Random_age'],ascending=[False,True]).reset_index(drop=True)
        df_out = df_out.reset_index(drop=False)
        df_out.rename(columns={'index':'Age rank'},inplace=True)
        
        df_out = df_out.loc[:,['Plant ID','Age','Age rank']]
        df_out.to_csv('../output/3_AgeRank/'+isec+'_'+ifa+'.csv',index=None,encoding='utf-8-sig')