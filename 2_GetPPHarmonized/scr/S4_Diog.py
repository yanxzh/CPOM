# -*- coding: utf-8 -*-
"""
Created on Sat Mar 30 23:30:15 2024

@author: 92978
"""

#%%
import pandas as pd
import numpy as np
#import sys
import time
import os
import datetime
import matplotlib.pyplot as plt
from matplotlib.pyplot import MultipleLocator
from matplotlib import cm
import matplotlib as mpl
mpl.rcParams['font.sans-serif'] = ["STSong"];
mpl.rcParams["axes.unicode_minus"] = False;
plt.rc('font',size=50,family="Arial");
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
import shutil
from S0_GlobalENV import *

#%%
def ener_plot(sec,fa,engsc):
    harmo_trend = pd.read_csv('../output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_RegionTrend.csv')
    ccs_trend = pd.read_csv('../output/3_HarmonizedTrend/'+ieng_sc+'/'+isec+'_'+ifa+'_CCS_RegionTrend.csv')
    harmo_trend.index = harmo_trend['Region']
    ccs_trend.index = ccs_trend['Region']
    
    region = ['Canada+Latin America',
                'China',
                'East Asia',
                'India',
                'Middle East and Africa',
                'Other Asia and Pacific',
                'Russia+Eastern Europe',
                'United States',
                'Western Europe']
    
    ccs_trend = ccs_trend.reindex(index=region,fill_value=np.nan)
    harmo_trend = harmo_trend.reindex(index=region,fill_value=np.nan)
    
    #基础设置
    font2 = {'family' : 'Arial','weight' : 'normal','size' : 50};
    color = ['#CF2B20','#EF9C13','#ECE836','#15793A','#7ABCCC']
    
    figsize=80,50
    fig, axis_ener = plt.subplots(3,4,figsize=figsize)
    plt.subplots_adjust(wspace=0.6,hspace=0.4)

    for ireg in range(len(region)):
        ax_ener = axis_ener[ireg//4,ireg%4]
        
        other_plot = harmo_trend.loc[harmo_trend['Region']==region[ireg],:]
        ccs_plot = ccs_trend.loc[ccs_trend['Region']==region[ireg],:]
        other_plot.loc[:,yearls.astype(str)] = other_plot.loc[:,yearls.astype(str)]-ccs_plot.loc[:,yearls.astype(str)]
        
        all_plot = pd.concat([ccs_plot,other_plot],axis=0)
        all_plot.reset_index(drop=True,inplace=True)
        
        if sec=='Power':
            ax_ener.stackplot(yearls,all_plot.loc[:,yearls.astype(str)]/10**9,labels=['CCS','None CCS'],colors=color)
            ax_ener.set_ylabel('Generation (TWh)',font2,labelpad=15);
            
        elif sec=='IronAndSteel':
            ax_ener.stackplot(yearls,all_plot.loc[:,yearls.astype(str)]/10**3,labels=['CCS','None CCS'],colors=color)
            ax_ener.set_ylabel('Iron production(Mt)',font2,labelpad=15);
            
        elif sec=='Cement':
            ax_ener.stackplot(yearls,all_plot.loc[:,yearls.astype(str)]/10**3,labels=['CCS','None CCS'],colors=color)
            ax_ener.set_ylabel('Clinker production (Mt)',font2,labelpad=15);

        ax_ener.set_title(region[ireg],{'family':'Arial','weight':'normal','size':60},pad=50);
        
        ax_ener.tick_params(which='both', length=20, labelsize=40,width=3)
        labels = ax_ener.get_xticklabels() + ax_ener.get_yticklabels() #+ ax1.get_yticklabels();
        [label.set_fontname("Arial") for label in labels];
        
        ax_ener.set_xlim([yearls.values[0],yearls[-1:].values]);
        #ax.set_ylim([0,max(coun_emis.loc[:,yearls].sum())*1.5]); #y轴强制从0开始        str(final_data.iloc[iday,0])[]
        ax_ener.xaxis.set_major_locator(MultipleLocator(10));
        
        bwith = 3
        ax_ener.spines['bottom'].set_linewidth(bwith);
        ax_ener.spines['left'].set_linewidth(bwith);
        ax_ener.spines['top'].set_linewidth(bwith);
        ax_ener.spines['right'].set_linewidth(bwith);
        
    other_plot = harmo_trend.drop('Region',axis=1).sum()
    ccs_plot = ccs_trend.drop('Region',axis=1).sum()
    other_plot = other_plot-ccs_plot
    
    all_plot = pd.concat([ccs_plot,other_plot],axis=1).T
            
    ax_ener = axis_ener[2,1]

    if sec=='Power':
        ax_ener.stackplot(yearls,all_plot.loc[:,yearls.astype(str)]/10**9,labels=['CCS','None CCS'],colors=color)
        ax_ener.set_ylabel('Generation (TWh)',font2,labelpad=15);
        
    elif sec=='IronAndSteel':
        ax_ener.stackplot(yearls,all_plot.loc[:,yearls.astype(str)]/10**3,labels=['CCS','None CCS'],colors=color)
        ax_ener.set_ylabel('Iron production(Mt)',font2,labelpad=15);
        
    elif sec=='Cement':
        ax_ener.stackplot(yearls,all_plot.loc[:,yearls.astype(str)]/10**3,labels=['CCS','None CCS'],colors=color)
        ax_ener.set_ylabel('Clinker production (Mt)',font2,labelpad=15);
            
    ax_ener.set_title('Global',{'family':'Arial','weight':'normal','size':60},pad=50);
    
    ax_ener.tick_params(which='both', length=20, labelsize=40,width=3)
    labels = ax_ener.get_xticklabels() + ax_ener.get_yticklabels() #+ ax1.get_yticklabels();
    [label.set_fontname("Arial") for label in labels];
    
    ax_ener.set_xlim([yearls.values[0],yearls[-1:].values]);
    #ax.set_ylim([0,max(coun_emis.loc[:,yearls].sum())*1.5]); #y轴强制从0开始        str(final_data.iloc[iday,0])[]
    ax_ener.xaxis.set_major_locator(MultipleLocator(10));
    
    bwith = 3 
    ax_ener.spines['bottom'].set_linewidth(bwith);
    ax_ener.spines['left'].set_linewidth(bwith);
    ax_ener.spines['top'].set_linewidth(bwith);
    ax_ener.spines['right'].set_linewidth(bwith);
    
    ax_ener = axis_ener[2,2]
    ax_ener.axis('off')
    ax_ener = axis_ener[2,3]
    ax_ener.axis('off')
    
    ax_lable = fig.add_axes([0.55, 0.3, 0.1, 0.02])
    ax_lable.stackplot(yearls,all_plot.loc[:,yearls.astype(str)],labels=['CCS','None CCS'],colors=color)
    ax_lable.legend(bbox_to_anchor=(0.05, 0.96), loc='upper left',ncol=1,
              borderaxespad=0., handlelength=1.5,
              prop={'family' : 'Arial','weight' : 'bold','size' : 50},frameon=False);
    ax_lable.set_xlim([-1,-1]);
    ax_lable.set_ylim([-1,-1]);
    
    ax_lable.axis('off');
    
    plt.suptitle(sec+'_'+fa,y=0.94,fontsize=70,family="Arial",weight='bold',style='italic');
    plt.savefig('../output/4_HarmonizedDiog/'+engsc+'/'+isec+'_'+ifa+'_ProdTrend.jpg',dpi=50, bbox_inches='tight');
    
    #防止内存溢出
    plt.clf();
    plt.close();
    plt.cla();
        
    return

#%%
for ieng_sc in Eng_scenarios:
    mkdir('../output/4_HarmonizedDiog/'+ieng_sc)
    for isec in ['Cement','Power','IronAndSteel']:
        # #涉及到的设备
        faciliy = fa_dict.loc[fa_dict['Sector']==isec,'Facility Type']
        
        for ifa in faciliy:
            ener_plot(sec=isec,fa=ifa,engsc=ieng_sc)