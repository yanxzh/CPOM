#!/bin/bash

# 实验组名字列表
experiment_groups=("Age2Capacity" "Death" "BAU" "Carbon")

# 循环遍历实验组
for group in "${experiment_groups[@]}"
do
# 构造任务名
job_name="${group}"

# 构造输出和错误文件名
output_file="stdout.$job_name"
error_file="stderr.$job_name"

# 提交任务
sbatch <<-EOM
#!/bin/bash
#SBATCH -J $job_name
#SBATCH -N 1
#SBATCH -p cnall
#SBATCH -o $output_file
#SBATCH -e $error_file
#SBATCH --no-requeue
#SBATCH --ntasks-per-node=56

source /home/tongdan/anaconda3/etc/profile.d/conda.sh
conda activate coal_power_yxz

#date
#cd /WORK/tongdan_work/yanxizhe/Coal_power/分组测试_3/DPEC_MC_${group}/5_PPTurnover/scr/
#python S2_RunAll.py>/WORK/tongdan_work/yanxizhe/Coal_power/分组测试_3/DPEC_MC_${group}/7_linux_run/log_pathway

#date
#cd /WORK/tongdan_work/yanxizhe/Coal_power/分组测试_3/DPEC_MC_${group}/5_PPTurnover_HistorialRetirement/scr/
#python S2_RunAll.py>/WORK/tongdan_work/yanxizhe/Coal_power/分组测试_3/DPEC_MC_${group}/7_linux_run/log_historical

date
cd /WORK/tongdan_work/yanxizhe/Coal_power/分组测试_3/DPEC_MC_${group}/6_Scenario_analysis/scr/
python S1M_Run_all.py>/WORK/tongdan_work/yanxizhe/Coal_power/分组测试_3/DPEC_MC_${group}/7_linux_run/log_collection

date
printf "Scenario finish"
        
EOM
        
done