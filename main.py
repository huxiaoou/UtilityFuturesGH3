import argparse
import datetime as dt
from DbMajorMinor import cal_major_minor_mp
from utility_futures_setup import global_config, futures_md_structure_path, futures_md_db_name, futures_md_dir, \
    calendar_path, major_minor_dir

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description="Entry point of this project", formatter_class=argparse.RawTextHelpFormatter)
    args_parser.add_argument("-w", "--switch", type=str,
                             help="""use this to decide which parts to run, available options = {
    'mm': major_minor,
    'mr': major_return,
    'fd': fundamental,
    }""")
    args_parser.add_argument("-p", "--process", type=int, default=5, help="""number of process to be called when calculating, default = 5""")
    args_parser.add_argument("-m", "--mode", type=str, choices=("o", "a"), help="""run mode, available options = {'o', 'overwrite', 'a', 'append'}""")
    args_parser.add_argument("-b", "--bgn", type=str, help="""begin date, suggested = '20120101'. """)
    args_parser.add_argument("-s", "--stp", type=str, help="""stop date, not included.""")
    args_parser.add_argument("-r", "--source", type=str, choices=("sql", "tsdb"), help="""type of source data""")

    args = args_parser.parse_args()
    switch = args.switch.upper()
    proc_num = args.process
    run_mode = None if switch in ["FD"] else args.mode.upper()
    bgn_date, stp_date = args.bgn, args.stp
    stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d") if stp_date is None else stp_date
    src_tab_name = "CTable2" if args.source.upper() == "TSDB" else "CTable"

    concerned_universe = global_config["futures"]["concerned_universe"]
    volume_mov_ave_n_config, volume_mov_ave_n_default = {"IH.CFE": 1, "IF.CFE": 1, "IC.CFE": 1, "IM.CFE": 1,
                                                         "TS.CFE": 1, "T.CFE": 1, "TF.CFE": 1, "TL.CFE": 1, }, 3

    if switch in ["MM"]:
        cal_major_minor_mp(
            instrument_ids=concerned_universe,
            db_save_dir=major_minor_dir, db_save_name="major_minor.db",
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            src_tab_name=src_tab_name,
            futures_md_structure_path=futures_md_structure_path,
            futures_md_db_name=futures_md_db_name,
            futures_md_dir=futures_md_dir,
            calendar_path=calendar_path,
            volume_mov_ave_n_config=volume_mov_ave_n_config,
            volume_mov_ave_n_default=volume_mov_ave_n_default,
            verbose=False
        )
    elif switch in ["AU"]:  # "AVAILABLE UNIVERSE"
        pass
    else:
        print(f"... switch = {switch} is not a legal option, please check again.")
