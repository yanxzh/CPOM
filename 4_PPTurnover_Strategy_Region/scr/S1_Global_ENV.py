
#%%
#import pandas as pd
#import numpy as np
import sys
import os
 
#%%
#新建文件夹
def mkdir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)            
		

#%%
sys.path.append(r"../../0_SetAndRun")
from S1_RunSet import *

INPUT_PATH = '../input/'+dir_prefix

OUTPUT_PATH = '../output/'+dir_prefix
mkdir(OUTPUT_PATH)

FIGURE_PATH = '../figure/'+dir_prefix
mkdir(OUTPUT_PATH)

UNCER_POL_PATH = '../../4_UncerPrepare/output/test1/'

Order_scenarios = ['Cost']

ireg = 'China'
