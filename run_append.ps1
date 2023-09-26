$proc_num = 5
$bgn_date = 20120101
$append_date = Read-Host -Prompt "Please input the append date, which is LAST trade date. format = [YYYYMMDD]"
$append_date_dt = [Datetime]::ParseExact($append_date, "yyyyMMdd", $null)
$stp_date = Get-Date $append_date_dt.AddDays(1) -Format "yyyyMMdd"
$src_database = "wds"

python main.py -p $proc_num -w mm  -m a -b $append_date -r $src_database
python main.py -p $proc_num -w mr  -m a -b $append_date -r $src_database

python main.py -p $proc_num -w md  -m a -b $append_date -r $src_database
python main.py -p $proc_num -w fd       -b $bgn_date    -r $src_database -s $stp_date

python main.py -p $proc_num -w vol -m a -b $append_date -r $src_database
python main.py -p $proc_num -w mbr -m a -b $append_date
