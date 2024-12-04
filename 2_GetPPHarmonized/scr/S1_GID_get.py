# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 14:04:10 2024

@author: 92978

"""

#%%
import numpy as np
import pandas as pd
from S0_GlobalENV import *

#%%
def get_fac_data(df,sec,mk):
    #提取有用信息    
    ########################################
    col = ['Plant ID', 'Country', 'Longitude', 'Latitude', 'Plant Name', 'Sector',
           'Facility ID', 'Facility Type', 'Fuel Type', 'Start Year', 'Close Year',
           'Capacity', 'Capacity Unit', 'Year', 'Activity type',
           'Activity rates Unit', 'CO2 Eta (%)',]
    gid_info = df.loc[:,col].drop_duplicates(['Facility ID'])

    gid_emis = df.loc[:,['Facility ID','Activity rates','CO2 Emissions']].groupby(['Facility ID'],as_index=False).sum()
    final_facility_data = pd.merge(gid_info,gid_emis,on='Facility ID',how='left')
    
    if mk=='prod':
        final_facility_data.rename(columns={'CO2 Emissions':'CO2 Emissions_prod',
                                            'Activity rates':'Production',
                                            'Activity rates Unit':'Production Unit'},inplace=True)
    elif mk=='ener':
        final_facility_data.rename(columns={'CO2 Emissions':'CO2 Emissions_ener'},inplace=True)
    
    return final_facility_data

def get_iron(isec):
    
    print(isec,flush=True)
    gid_data = pd.read_pickle('../input/PP/GID_database_'+isec+'.pkl')
    gid_data = gid_data.loc[(gid_data['ID3 Name']!='fugitive'),:].reset_index(drop=True)
    
    use_col = ['Plant ID','Country','Longitude','Latitude',
               'Plant Name', 'Sector', 'Facility ID', 'Facility Type',
               'Fuel Type', 'Start Year', 'Close Year', 'Capacity', 'Capacity Unit',
               'Year', 'Activity rates', 'Activity type', 'Activity rates Unit',
               'CO2 Eta (%)', 'CO2 Emissions']
    
    gid_data.loc[gid_data['Capacity Unit']=='kt','Capacity'] = gid_data.loc[gid_data['Capacity Unit']=='kt','Capacity']/10**3
    gid_data.loc[gid_data['Capacity Unit']=='kt','Capacity Unit'] = 'Mt'
    
    filtered = (pd.isnull(gid_data['Longitude'])==0)&(gid_data['Year']==2020)&(gid_data['Longitude']!=' ')
    gid_data = gid_data.loc[filtered,use_col].reset_index(drop=True)
    
    for irow in ['Longitude','Latitude','CO2 Emissions','Capacity','Activity rates']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(float)
    del irow
    for irow in ['Start Year','Close Year','Year']:
        gid_data.loc[:,irow] =  gid_data.loc[:,irow].replace(' ',9999)
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(int)
    del irow
    
    gid_data = gid_data.loc[~(gid_data['Longitude']<-180),:].reset_index(drop=True)
    
    gid_data_pro = gid_data.loc[gid_data['Activity type']=='Production',:].reset_index(drop=True)
    gid_data_comb = gid_data.loc[gid_data['Activity type']=='Energy Consumption',:].reset_index(drop=True)
    
    gid_data_pro = get_fac_data(df=gid_data_pro,sec=isec,mk='prod')
    gid_data_comb = get_fac_data(df=gid_data_comb,sec=isec,mk='ener')
    
    gid_data = pd.merge(gid_data_comb,
                        gid_data_pro.loc[:,['Facility ID','Production','Production Unit','CO2 Emissions_prod']],
                        on='Facility ID',how='left')

    gid_data['Production Unit'] = 'kt'
    gid_data.loc[:,['Production','CO2 Emissions_prod']] = gid_data.loc[:,['Production','CO2 Emissions_prod']].fillna(0)
    gid_data['CO2 Emissions'] = gid_data['CO2 Emissions_prod']+gid_data['CO2 Emissions_ener']
    gid_data.drop(['CO2 Emissions_prod','CO2 Emissions_ener'],axis=1,inplace=True)
    
    age_get_is = gid_data.loc[:,['Plant ID','Start Year','CO2 Emissions']].copy(deep=True)
    age_get_is['Age'] = yearls[0]-age_get_is['Start Year'].astype(int)
    age_get_is['Age_Emis'] = age_get_is['Age']*age_get_is['CO2 Emissions']
    age_get_is = age_get_is.loc[:,['Plant ID','Age_Emis','CO2 Emissions']].groupby(['Plant ID'],as_index=False).sum()
    age_get_is['Age'] = age_get_is['Age_Emis']/age_get_is['CO2 Emissions']
    age_get_is = age_get_is.loc[:,['Plant ID','Age']]
    
    gid_data.loc[np.isin(gid_data['Facility Type'],['DRI','BF','Other'])==0,['Capacity','Production']] = 0
    col = ['Plant ID', 'Country', 'Longitude', 'Latitude', 'Plant Name', 'Sector',
           'Facility ID', 'Facility Type', 'Fuel Type', 'Start Year', 'Close Year',
           'Capacity Unit', 'Year', 'Activity type', 
           'Activity rates Unit', 'CO2 Eta (%)','Production Unit']
    gid_info = gid_data.loc[:,col].drop_duplicates(['Plant ID'])

    gid_emis = gid_data.loc[:,['Plant ID','Activity rates','Production','CO2 Emissions','Capacity']].groupby(['Plant ID'],as_index=False).sum()
    final_plant_data = pd.merge(gid_info,gid_emis,on='Plant ID',how='left')
    final_plant_data = pd.merge(final_plant_data,age_get_is,on='Plant ID',how='left')
    final_plant_data['Start Year'] = yearls[0]-final_plant_data['Age']
    
    final_plant_data['Facility Type'] = 'Iron'
    final_plant_data['Sector'] = 'IronAndSteel'
    # country_error = gid_data.loc[np.isin(gid_data['Country'],pd.Series(coun_ls))==0,'Country'].drop_duplicates()
    
    final_plant_data = final_plant_data.loc[final_plant_data['Production']>0,:].reset_index(drop=True)
    
    final_plant_data.to_pickle('../output/1_PlantLevelPP/'+isec+'.pkl')
    final_plant_data.to_csv('../output/1_PlantLevelPP/'+isec+'.csv',index=None,encoding='utf-8-sig')
    
    return

def get_cement(isec):
    print(isec,flush=True)
    gid_data = pd.read_pickle('../input/PP/GID_database_'+isec+'.pkl')
    gid_data = gid_data.loc[(gid_data['ID2 Name']!='Cement production'),:].reset_index(drop=True)
    
    use_col = ['Plant ID','Country','Longitude','Latitude',
               'Plant Name', 'Sector', 'Facility ID', 'Facility Type',
               'Fuel Type', 'Start Year', 'Close Year', 'Capacity', 'Capacity Unit',
               'Year', 'Activity rates', 'Activity type', 'Activity rates Unit',
               'CO2 Eta (%)', 'CO2 Emissions']
    
    filtered = (gid_data['CO2 Emissions']>0)&(pd.isnull(gid_data['Longitude'])==0)&(gid_data['Year']==2020)
    gid_data = gid_data.loc[filtered,use_col].reset_index(drop=True)
    
    #数据格式转换
    for irow in ['Longitude','Latitude','CO2 Emissions','Capacity','Activity rates']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(float)
    del irow
    for irow in ['Start Year','Close Year','Year']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(int)
    del irow
    
    gid_data_pro = gid_data.loc[(gid_data['Activity type']=='Clinker Production'),:].reset_index(drop=True)
    gid_data_comb = gid_data.loc[gid_data['Activity type']=='Energy Consumption',:].reset_index(drop=True)
    
    gid_data_pro = get_fac_data(df=gid_data_pro,sec=isec,mk='prod')
    gid_data_comb = get_fac_data(df=gid_data_comb,sec=isec,mk='ener')
    
    gid_data = pd.merge(gid_data_pro,
                        gid_data_comb.loc[:,['Facility ID','Activity rates','Activity rates Unit','CO2 Emissions_ener']],
                        on='Facility ID',how='outer')
    
    gid_data['Production Unit'] = 'kt'
    gid_data.loc[:,['Activity rates','CO2 Emissions_ener','Production','CO2 Emissions_prod']] =\
        gid_data.loc[:,['Activity rates','CO2 Emissions_ener','Production','CO2 Emissions_prod']].fillna(0)
    gid_data['CO2 Emissions'] = gid_data['CO2 Emissions_prod']+gid_data['CO2 Emissions_ener']
    gid_data.drop(['CO2 Emissions_prod','CO2 Emissions_ener'],axis=1,inplace=True)
    
    age_get_ce = gid_data.loc[:,['Plant ID','Start Year','Capacity']].copy(deep=True)
    age_get_ce['Age'] = yearls[0]-age_get_ce['Start Year'].astype(int)
    age_get_ce['Age_Cap'] = age_get_ce['Age']*age_get_ce['Capacity']
    age_get_ce = age_get_ce.loc[:,['Plant ID','Age_Cap','Capacity']].groupby(['Plant ID'],as_index=False).sum()
    age_get_ce['Age'] = age_get_ce['Age_Cap']/age_get_ce['Capacity']
    age_get_ce = age_get_ce.loc[:,['Plant ID','Age']]
    
    col = ['Plant ID', 'Country', 'Longitude', 'Latitude', 'Plant Name', 'Sector',
           'Facility ID', 'Facility Type', 'Fuel Type', 'Start Year', 'Close Year',
           'Capacity Unit', 'Year', 'Activity type', 
           'Activity rates Unit', 'CO2 Eta (%)','Production Unit']
    gid_info = gid_data.loc[:,col].drop_duplicates(['Plant ID'])

    gid_emis = gid_data.loc[:,['Plant ID','Activity rates','Production','CO2 Emissions','Capacity']].groupby(['Plant ID'],as_index=False).sum()
    final_plant_data = pd.merge(gid_info,gid_emis,on='Plant ID',how='left')
    final_plant_data = pd.merge(final_plant_data,age_get_ce,on='Plant ID',how='left')
    final_plant_data['Start Year'] = yearls[0]-final_plant_data['Age']
    
    final_plant_data['Facility Type'] = 'Clinker'
    final_plant_data['Sector'] = 'Cement'

    final_plant_data.to_pickle('../output/1_PlantLevelPP/'+isec+'.pkl')
    final_plant_data.to_csv('../output/1_PlantLevelPP/'+isec+'.csv',index=None,encoding='utf-8-sig')
    
    return

def get_power(isec):
    
    print(isec,flush=True)
    gid_data = pd.read_pickle('../input/PP/GID_database_'+isec+'.pkl')
    use_col = ['Plant ID','Country','Longitude','Latitude',
               'Plant Name', 'Sector', 'Facility ID', 'Facility Type',
               'Fuel Type', 'Start Year', 'Close Year', 'Capacity', 'Capacity Unit',
               'Year', 'Activity rates', 'Activity type', 'Activity rates Unit',
               'CO2 Eta (%)','CO2 Emissions','Generation','Generation_Unit']
    
    filtered = (gid_data['CO2 Emissions']>0)&(pd.isnull(gid_data['Longitude'])==0)&(gid_data['Year']==2020)
    gid_data = gid_data.loc[filtered,use_col].reset_index(drop=True)

    for irow in ['Longitude','Latitude','CO2 Emissions','Capacity','Activity rates']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(float)
    del irow
    for irow in ['Start Year','Close Year','Year']:
        gid_data.loc[:,irow] = gid_data.loc[:,irow].astype(int)
    del irow
    
    gid_data.rename(columns={'Generation':'Production','Generation_Unit':'Production Unit'},
                    inplace=True)
    
    gid_data['Capacity'] = gid_data['Capacity']*8760
    gid_data['Capacity Unit'] = 'MWh'
    gid_data['Production'] = gid_data['Production']*10**6
    gid_data['Production Unit'] = 'KWh'
    
    age_get_po = gid_data.loc[:,['Plant ID','Start Year','Capacity']].copy(deep=True)
    age_get_po['Age'] = yearls[0]-age_get_po['Start Year'].astype(int)
    age_get_po['Age_Cap'] = age_get_po['Age']*age_get_po['Capacity']
    age_get_po = age_get_po.loc[:,['Plant ID','Age_Cap','Capacity']].groupby(['Plant ID'],as_index=False).sum()
    age_get_po['Age'] = age_get_po['Age_Cap']/age_get_po['Capacity']
    age_get_po = age_get_po.loc[:,['Plant ID','Age']]
    
    col = ['Plant ID', 'Country', 'Longitude', 'Latitude', 'Plant Name', 'Sector',
           'Facility ID', 'Facility Type', 'Fuel Type', 'Start Year', 'Close Year',
           'Capacity Unit', 'Year', 'Activity type',
           'Activity rates Unit', 'CO2 Eta (%)','Production Unit']
    gid_info = gid_data.loc[:,col].drop_duplicates(['Plant ID'])

    gid_emis = gid_data.loc[:,['Plant ID','Activity rates','Production','CO2 Emissions','Capacity']].groupby(['Plant ID'],as_index=False).sum()
    final_plant_data = pd.merge(gid_info,gid_emis,on='Plant ID',how='left')
    final_plant_data = pd.merge(final_plant_data,age_get_po,on='Plant ID',how='left')
    final_plant_data['Start Year'] = yearls[0]-final_plant_data['Age']
    
    final_plant_data.to_pickle('../output/1_PlantLevelPP/'+isec+'.pkl')
    final_plant_data.to_csv('../output/1_PlantLevelPP/'+isec+'.csv',index=None,encoding='utf-8-sig')
    
    return

    #%%
if __name__ == '__main__':
    get_iron(isec='IronAndSteel')
    get_cement(isec='Cement')
    get_power(isec='Power')