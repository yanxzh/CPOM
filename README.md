# CPOM
This folder contains the project code for CCUS-layout-resolved Pathway Optimization Model (CPOM), including:

0_SetAndRun: basic setting for the modelling

1_GCAMScenario: Extracting GCAM-based projections of industrial outputs for key industries and interpolating them to the defined time resolution (e.g., annual intervals).

2_GetPPHarmonized: Harmonizing projections of industrial outputs by using the Global Infrastructure emissions Detector (GID) data and extracting technical information for current facilities.

3_CandidateNetwork: Segmenting data by regions and constructing a candidate CCUS network based on Open street map (OSM) and gas pipeline networks.

4_PPTurnover_Strategy_Region: Modeling future possible turnover and projecting for CCUS-layout-resolved transition pathway by each region under each strategy.

We provide full access to all the code used in generating the results for the study "CCUS layout reshapes energy infrastructure transition for global key industries." However, some of the data utilized in the CPOM model is not publicly available, as it depends on technical attributes and emission data obtained from GID, which is based on proprietary databases from collaborators. These databases, including WEPP, MCI, and Global Cement, are subject to user license agreements that restrict public access.

If you are looking for more details, please contact yanxz22@mails.tsinghua.edu.cn or dantong@tsinghua.edu.cn.
