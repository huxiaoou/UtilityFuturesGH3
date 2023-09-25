$proc_num = 5
$bgn_date = 20120101
$stp_date = Read-Host -Prompt "Please input the stop date, which is NOT INCLUDED. format = [YYYYMMDD]"
$src_database = "wds"

python main.py -p $proc_num -w mm  -m o -b $bgn_date -s $stp_date -r $src_database
python main.py -p $proc_num -w mr  -m o -b $bgn_date -s $stp_date -r $src_database

python main.py -p $proc_num -w md  -m o -b $bgn_date -s $stp_date -r $src_database 
python main.py -p $proc_num -w fd       -b $bgn_date -s $stp_date -r $src_database

python main.py -p $proc_num -w vol -m o -b $bgn_date -s $stp_date -r $src_database
python main.py -p $proc_num -w mbr -m o -b $bgn_date -s $stp_date

