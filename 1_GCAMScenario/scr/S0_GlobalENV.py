# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 09:47:12 2023

@author: yanxizhe
"""

#%%
#import pandas as pd
#import numpy as np
import sys
import os
 
#%%
#新建文件夹
def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   #判断是否存在文件夹如果不存在则创建为文件夹
		os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
		

#%%
sys.path.append(r"../../0_SetAndRun")
from S1_RunSet import *

#输入路径
INPUT_PATH = '../input/'+dir_prefix

#工作路径
OUTPUT_PATH = '../output/'+dir_prefix
mkdir(OUTPUT_PATH)

# #多核运行方式
# MultiWay = 'Sector'