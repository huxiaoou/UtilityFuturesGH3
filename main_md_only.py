"""
0.  Main entry point of this project. Run this script to reformat futures data and make some
    necessary preparation for further research. The results of this script are shared by and
    basic input of many following projects.
1.  Two modes are allowed
    A.  mode = o, overwrite. Reformat the data between the beginning date(argument[2])
        and the stop date(argument[3]). The stop date is NOT included.
        For example:
            python main_md_only.py sql  o 20120101 20230307
            python main_md_only.py tsdb o 20120101 20230307
        Program would remove all the existing data in the database, and download all the data
        between [20120101, 20230307).
        Mostly used to download all the historical data.
    B.  mode = a, append. Reformat the data of the specific date provided in the argument[2].
        For example:
            python main_md_only.py sql  a 20230321
            python main_md_only.py tsdb a 20230321
        Program would reformat all the data of the specific date (20230321) and add them to
        the database, while keeping all the existing ones.
        Mostly used to daily update.
2.   Two data source are provided, sql or TSDB
"""

from utility_futures_setup import sys
from utility_futures_setup import futures_md_structure_path, futures_md_db_name, futures_md_dir
from utility_futures_setup import major_minor_dir, major_return_dir, instru_idx_dir
from utility_futures_setup import md_by_instru_dir
from utility_futures_setup import calendar_path, futures_instru_info_path, global_config
from skyrim.whiterun import CCalendar, CInstrumentInfoTable
from DbMajorMinor import update_major_minor
from DbMajorReturn import update_major_return

CMDTY_VOLUME_MOVING_AVER_N, CMDTY_VOLUME_SHIFT_N = 3, 1
BONDS_VOLUME_MOVING_AVER_N, BONDS_VOLUME_SHIFT_N = 3, 1
INDEX_VOLUME_MOVING_AVER_N, INDEX_VOLUME_SHIFT_N = 1, 0
FIX_BGN_DATE = "20120101"
PRICE_TYPES = ["open", "close", "settle"]

# major contract is chosen based on the volume of all the
# contracts of the instrument during the past VOLUME_MOVING_AVER_N days.

calendar = CCalendar(calendar_path)
instru_info_table = CInstrumentInfoTable(t_path=futures_instru_info_path, t_type="CSV")

# --- check arguments
src_type = sys.argv[1].upper()
run_mode = sys.argv[2].upper()
if run_mode not in ["O", "OVERWRITE", "A", "APPEND"]:
    print("Not a right mode, please check again. Legal options is one of them: ['o', 'overwrite', 'a', 'append']")
    sys.exit()
bgn_date = sys.argv[3]
stp_date = sys.argv[4] if run_mode in ["O", "OVERWRITE"] else calendar.get_next_date(bgn_date, 1)
src_tab_name = "CTable" if src_type == "SQL" else "CTable2"

# --- main
concerned_universe = global_config["futures"]["concerned_universe"]
for i, instrument in enumerate(concerned_universe):
    instru, _ = instrument.split(".")
    if instru.upper() in ["IC", "IF", "IH", "IM"]:
        vol_mov_ave, vol_shift = INDEX_VOLUME_MOVING_AVER_N, INDEX_VOLUME_SHIFT_N
    elif instru.upper() in ["T", "TF", "TS", "TL"]:
        vol_mov_ave, vol_shift = BONDS_VOLUME_MOVING_AVER_N, BONDS_VOLUME_SHIFT_N
    else:
        vol_mov_ave, vol_shift = CMDTY_VOLUME_MOVING_AVER_N, CMDTY_VOLUME_SHIFT_N

    print("=" * 120)
    print("| SN = {:>2d}".format(i))
    update_major_minor(
        t_instrument_id=instrument,
        t_run_mode=run_mode, t_bgn_date=bgn_date, t_stp_date=stp_date,
        t_src_tab_name=src_tab_name,
        t_volume_moving_aver_n=vol_mov_ave, t_volume_shift_n=vol_shift,
        t_futures_md_structure_path=futures_md_structure_path,
        t_futures_md_db_name=futures_md_db_name,
        t_futures_md_dir=futures_md_dir,
        t_major_minor_dir=major_minor_dir,
    )

    # print("-" * 120)
    update_major_return(
        t_instrument_id=instrument,
        t_run_mode=run_mode, t_bgn_date=bgn_date, t_stp_date=stp_date,
        t_src_tab_name=src_tab_name,
        t_futures_md_structure_path=futures_md_structure_path,
        t_futures_md_db_name=futures_md_db_name,
        t_futures_md_dir=futures_md_dir,
        t_major_minor_dir=major_minor_dir,
        t_major_return_dir=major_return_dir,
        t_mkt_idx_dir=instru_idx_dir,
    )

    print("-" * 120 + "\n")
