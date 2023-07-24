"""
0.  Main entry point of this project. Run this script to reformat futures data and make some
    necessary preparation for further research. The results of this script are shared by and
    basic input of many following projects.
1.  Two data source are provided
    A.  tsdb, for example:
            python main_fundamental_only.py tsdb 20120101 20230717
    B.  sql, for example:
            python main_fundamental_only.py sql  20120101 20230717
"""

import sys
import datetime as dt
from utility_futures_setup import fundamental_by_instru_dir, futures_fundamental_dir, futures_fundamental_db_name
from utility_futures_setup import calendar_path, futures_instru_info_path, custom_ts_db_path
from skyrim.whiterun import CCalendar, CInstrumentInfoTable

t0 = dt.datetime.now()

# --- check arguments
# we suggest bgn_date = "20120101"
src_type, bgn_date, stp_date = sys.argv[1].upper(), sys.argv[2], sys.argv[3]

calendar = CCalendar(calendar_path)

# --- main
if src_type == "TSDB":
    from TSDBTranslator2.translator import CTSDBReader
    from cal_fundamental import update_fundamental_by_instrument

    instru_info_table = CInstrumentInfoTable(t_path=futures_instru_info_path, t_type="CSV")
    tsdb_reader = CTSDBReader(t_tsdb_path=custom_ts_db_path)
    update_fundamental_by_instrument(
        t_bgn_date=bgn_date, t_stp_date=stp_date,
        t_tsdb_reader=tsdb_reader,
        t_calendar=calendar,
        t_instru_info_table=instru_info_table,
        t_fundamental_by_instru_dir=fundamental_by_instru_dir,
    )
elif src_type == "SQL":
    from skyrim.falkreath import CManagerLibReader
    from cal_fundamental_from_sql import update_fundamental_by_instrument_from_sql

    sql_reader = CManagerLibReader(t_db_save_dir=futures_fundamental_dir, t_db_name=futures_fundamental_db_name + ".db")
    update_fundamental_by_instrument_from_sql(
        t_bgn_date=bgn_date, t_stp_date=stp_date,
        t_sql_reader=sql_reader,
        t_calendar=calendar,
        t_fundamental_by_instru_dir=fundamental_by_instru_dir,
    )
else:
    print("Not a right source data type, please check again. Legal options is one of them: ['tsdb', 'sql']")
    sys.exit()

t1 = dt.datetime.now()
print("... fundamental data by instrument have been updated for")
print("... total time consuming :{:.2f} seconds".format((t1 - t0).total_seconds()))
