$proc_num = 5
$append_date = Read-Host -Prompt "Please input the append date, which is LAST trade date. format = [YYYYMMDD]"
$src_database = "sql"

python main.py -p $proc_num -w mm  -m a -b $append_date -r $src_database
python main.py -p $proc_num -w mr  -m a -b $append_date -r $src_database

python main.py -p $proc_num -w md  -m a -b $append_date -r $src_database
python main.py -p $proc_num -w fd       -b $append_date -r $src_database

python main.py -p $proc_num -w vol -m a -b $append_date -r $src_database
python main.py -p $proc_num -w mbr -m a -b $append_date
