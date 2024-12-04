# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 09:47:12 2023

@author: yanxizhe

DEPC运行选项设计
"""

#%%
import numpy as np;
import pandas as pd

#%%
startyr = 2020
endyr = 2050
gap = 1
yearls = pd.Series(np.linspace(startyr,endyr,int((endyr-startyr)/gap)+1)).astype(int)

Eng_scenarios = ['Neut']
End_scenarios = ['BAU']

dir_prefix = 'CCSGID'

eng_sca = 0

uncer_test = 0
test_num = 30

sensi_test = 0

new_build_style = 'orderly'

pp_run = ['Power','IronAndSteel','Cement']

fa_dict = pd.DataFrame((['Power','Coal'],['Power','Gas'],['Power','Oil'],
                        ['IronAndSteel','Iron'],['Cement','Clinker']),columns=['Sector','Facility Type'])

spe_run = ['CO2']

core_num_max = 56

coun_ls = pd.read_excel('../../1_GCAMScenario/input/dict/RegionMapping.xlsx')['Country'].tolist()
region_ls = pd.read_excel('../../1_GCAMScenario/input/dict/RegionMapping.xlsx')['Region_CCUS'].drop_duplicates().tolist()
