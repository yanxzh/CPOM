# -*- coding: utf-8 -*-
"""
Created on Sat Feb 18 14:10:55 2023

@author: 92978
"""

#%%
import pandas as pd
import numpy as np
import os
import time
from S1_Global_ENV import *
from multiprocessing import Process
        
#%%
from S3_PhaseOut import phaseout_main
from S4_NewDistribute import NewProdDistr_main
from S5_OldProdEmis import Old_ProdEmis_main
from S6_NewFuelEmis import NewFuelEmis_main
from S7_SSM import SSM_main
from S8_SSM_sec import SSM_sec_main
from S9_CostRank import CostRank_main
from S10_FlowTrace import FlowTrace_main
from S11_CCSretrofit import CCS_main
from S12_CCSCapacity import StorageChange_main

#%%
def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)

def mainprossing_main(ieng_sc,iend_sc,ior_sc,isec,ifa,ireg,yr_beg,yr_end):
    Turnover_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/0_Turnover/'
    NewProdDistr_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/1_NewBuilt/'
    OldProdEmis_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/2_OldProdEmis/'
    NewFuelEmis_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/3_NewFuelEmis/'
    SSM_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/4_SSM/'
    CCSInstall_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/6_CCSInstall/'
    CCSCost_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/7_CCSCCSCost/'

    mkdir(Turnover_dir)
    mkdir(NewProdDistr_dir)
    mkdir(OldProdEmis_dir)
    mkdir(NewFuelEmis_dir)
    mkdir(SSM_dir)
    mkdir(CCSInstall_dir)

    phaseout_main(ieng_sc=ieng_sc,iend_sc=iend_sc,ior_sc=ior_sc,
                  isec=isec,ifa=ifa,ireg=ireg,
                  Turnover_dir=Turnover_dir,CCSInstall_dir=CCSInstall_dir,
                  yr_beg=yr_beg,yr_end=yr_end)

    NewProdDistr_main(ieng_sc=ieng_sc,iend_sc=iend_sc,ior_sc=ior_sc,
                      isec=isec,ifa=ifa,ireg=ireg,
                      Turnover_dir=Turnover_dir,NewProdDistr_dir=NewProdDistr_dir,CCSInstall_dir=CCSInstall_dir,
                      yr_beg=yr_beg,yr_end=yr_end)

    Old_ProdEmis_main(ieng_sc=ieng_sc,iend_sc=iend_sc,ior_sc=ior_sc,
                      isec=isec,ifa=ifa,ireg=ireg,
                      Turnover_dir=Turnover_dir,OldProdEmis_dir=OldProdEmis_dir,CCSInstall_dir=CCSInstall_dir,
                      yr_beg=yr_beg,yr_end=yr_end)

    NewFuelEmis_main(ieng_sc=ieng_sc,iend_sc=iend_sc,ior_sc=ior_sc,
                      isec=isec,ifa=ifa,ireg=ireg,
                      NewProdDistr_dir=NewProdDistr_dir,OldProdEmis_dir=OldProdEmis_dir,NewFuelEmis_dir=NewFuelEmis_dir,CCSInstall_dir=CCSInstall_dir,
                      yr_beg=yr_beg,yr_end=yr_end)
    
    return

def main_drive(big,end,all_sce_set,rs):
    for i in range(big,end):
            mainprossing_main(ieng_sc=all_sce_set.loc['Eng_scenarios',i],
                              iend_sc=all_sce_set.loc['End_scenarios',i],
                              ior_sc=all_sce_set.loc['Order_scenarios',i],
                              isec=all_sce_set.loc['Sectors',i],
                              ifa=all_sce_set.loc['Facility Type',i],
                              ireg=all_sce_set.loc['Region',i],
                              yr_beg=all_sce_set.loc['yr_beg',i],
                              yr_end=all_sce_set.loc['yr_end',i])

    return

#%%
if __name__ == '__main__':
    int_time = time.time()

    for ieng_sc in Eng_scenarios:
        for iend_sc in End_scenarios:
            for ior_sc in Order_scenarios:
                Turnover_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/0_Turnover/'
                NewProdDistr_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/1_NewBuilt/'
                OldProdEmis_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/2_OldProdEmis/'
                NewFuelEmis_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/3_NewFuelEmis/'
                SSM_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/4_SSM/'
                CCSInstall_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/6_CCSInstall/'
                CCSCost_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/7_CCSCCSCost/'

                mkdir(Turnover_dir)
                mkdir(NewProdDistr_dir)
                mkdir(OldProdEmis_dir)
                mkdir(NewFuelEmis_dir)
                mkdir(SSM_dir)
                mkdir(CCSInstall_dir)

                for iyr_beg,iyr_end in zip([2020,2030,2040],[2030,2040,2050]):
                    print(str(iyr_beg))

                    all_sce_set = pd.DataFrame(data=None,index=['Eng_scenarios','End_scenarios','Order_scenarios','Sectors','Facility Type','Region'])
                    all_sce_set = all_sce_set.copy(deep=True)
                    sce_num = 0

                    for isec in pp_run:
                        for ifa in fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']:
                            all_sce_set = pd.concat([all_sce_set,
                                                     pd.DataFrame([ieng_sc,iend_sc,ior_sc,isec,ifa,ireg,iyr_beg,iyr_end],
                                                                  index=['Eng_scenarios','End_scenarios','Order_scenarios',
                                                                         'Sectors','Facility Type','Region','yr_beg','yr_end'],
                                                                  columns=[sce_num])],axis=1)
                            sce_num = sce_num + 1

                    core_num = 56

                    rs_list = [np.random.RandomState() for _ in range(core_num)]

                    main_drive(big=0,end=all_sce_set.shape[1],all_sce_set=all_sce_set,rs=rs_list[0])

                    NetworkStatus_dir = OUTPUT_PATH+'/'+ieng_sc+'/'+iend_sc+'/'+ior_sc+'/5_Network_'+str(iyr_end)+'/'
                    mkdir(NetworkStatus_dir)

                    SSM_main(ieng_sc,iend_sc,ior_sc,ireg,
                             NewFuelEmis_dir,SSM_dir,Turnover_dir,
                             iyr_beg,iyr_end)

                    SSM_sec_main(ieng_sc,iend_sc,ior_sc,ireg,NewFuelEmis_dir,SSM_dir,iyr_beg,iyr_end)

                    CostRank_main(ieng_sc,iend_sc,ior_sc,ireg,SSM_dir,NetworkStatus_dir,iyr_beg,iyr_end)

                    FlowTrace_main(ieng_sc,iend_sc,ior_sc,ireg,SSM_dir,NetworkStatus_dir,iyr_beg,iyr_end)

                    CCS_main(ieng_sc,iend_sc,ior_sc,ireg,
                              Turnover_dir,CCSInstall_dir,NewProdDistr_dir,NewFuelEmis_dir,NetworkStatus_dir,SSM_dir,
                              iyr_beg,iyr_end)

                    StorageChange_main(ieng_sc,iend_sc,ior_sc,ireg,
                                       CCSInstall_dir,SSM_dir,NetworkStatus_dir,
                                       iyr_beg,iyr_end)