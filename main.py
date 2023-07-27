import argparse
import datetime as dt
from skyrim.whiterun import CCalendar, CInstrumentInfoTable
from utility_futures_setup import (global_config,
                                   futures_md_structure_path, futures_md_db_name, futures_md_dir,
                                   futures_fundamental_structure_path, futures_fundamental_db_name, futures_fundamental_dir,
                                   custom_ts_db_path,
                                   calendar_path, futures_instru_info_path,
                                   futures_by_instrument_dir, major_minor_db_name, major_return_db_name, instrument_volume_db_name, instrument_member_db_name,
                                   md_by_instru_dir, fundamental_by_instru_dir,
                                   )

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
    src_type = args.source.upper()
    src_tab_name_md = "CTable2" if src_type == "TSDB" else "CTable"

    # manual config
    concerned_universe = global_config["futures"]["concerned_universe"]
    volume_mov_ave_n_config, volume_mov_ave_n_default = {"IH.CFE": 1, "IF.CFE": 1, "IC.CFE": 1, "IM.CFE": 1,
                                                         "TS.CFE": 1, "T.CFE": 1, "TF.CFE": 1, "TL.CFE": 1, }, 3
    major_return_price_type = "close"
    vo_adj_split_date = "20200101"
    price_types = ["open", "close", "settle"]
    src_tab_name_member = "CTablePositionC"

    # shared calendar and instru_info_table
    calendar = CCalendar(calendar_path)
    instru_info_table = CInstrumentInfoTable(t_path=futures_instru_info_path, t_type="CSV",
                                             t_index_label="windCode" if switch == "VOL" else "instrumentId")

    # main
    if switch in ["MM"]:
        from DbMajorMinor import CDbByInstrumentSQLMajorMinor

        db_by_instrument = CDbByInstrumentSQLMajorMinor(
            db_save_dir=futures_by_instrument_dir, db_save_name=major_minor_db_name, instrument_ids=concerned_universe,
            run_mode=run_mode,
            src_db_structure_path=futures_md_structure_path,
            src_db_name=futures_md_db_name,
            src_tab_name=src_tab_name_md,
            src_db_dir=futures_md_dir,
            volume_mov_ave_n_config=volume_mov_ave_n_config,
            volume_mov_ave_n_default=volume_mov_ave_n_default,
            calendar=calendar,
            verbose=False,
        )
        db_by_instrument.main_loop(instrument_ids=concerned_universe, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date)

        # cal_major_minor(
        #     db_save_dir=futures_by_instrument_dir, db_save_name=major_minor_db_name, instrument_ids=concerned_universe,
        #     run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
        #     futures_md_structure_path=futures_md_structure_path,
        #     futures_md_db_name=futures_md_db_name,
        #     src_tab_name=src_tab_name_md,
        #     futures_md_dir=futures_md_dir,
        #     volume_mov_ave_n_config=volume_mov_ave_n_config,
        #     volume_mov_ave_n_default=volume_mov_ave_n_default,
        #     calendar=calendar,
        #     verbose=False
        # )
    elif switch in ["MR"]:
        from DbMajorReturn import cal_major_return

        cal_major_return(
            db_save_dir=futures_by_instrument_dir, db_save_name=major_return_db_name, instrument_ids=concerned_universe,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            futures_md_structure_path=futures_md_structure_path,
            futures_md_db_name=futures_md_db_name,
            src_tab_name=src_tab_name_md,
            futures_md_dir=futures_md_dir,
            major_return_price_type=major_return_price_type,
            vo_adj_split_date=vo_adj_split_date,
            major_minor_dir=futures_by_instrument_dir,
            major_minor_db_name=major_minor_db_name,
            calendar=calendar,
            verbose=False
        )
    elif switch in ["MD"]:
        from DbMd import cal_md

        cal_md(
            proc_num=proc_num,
            md_by_instru_dir=md_by_instru_dir, price_types=price_types, instrument_ids=concerned_universe,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            futures_md_structure_path=futures_md_structure_path,
            futures_md_db_name=futures_md_db_name,
            src_tab_name=src_tab_name_md,
            futures_md_dir=futures_md_dir,
            calendar=calendar,
        )
    elif switch in ["FD"]:
        if src_type == "TSDB":
            from TSDBTranslator2.translator import CTSDBReader
            from CalFundFromTSDB import update_fundamental_by_instrument

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
            from CalFundFromSQL import update_fundamental_by_instrument_from_sql

            sql_reader = CManagerLibReader(t_db_save_dir=futures_fundamental_dir, t_db_name=futures_fundamental_db_name + ".db")
            update_fundamental_by_instrument_from_sql(
                t_bgn_date=bgn_date, t_stp_date=stp_date,
                t_sql_reader=sql_reader,
                t_calendar=calendar,
                t_fundamental_by_instru_dir=fundamental_by_instru_dir,
            )
    elif switch in ["VOL"]:
        from DbVolume import cal_volume

        cal_volume(
            db_save_dir=futures_by_instrument_dir, db_save_name=instrument_volume_db_name, instrument_ids=concerned_universe,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            src_db_structure_path=futures_md_structure_path,
            src_db_name=futures_md_db_name,
            src_tab_name=src_tab_name_md,
            src_db_dir=futures_md_dir,
            vo_adj_split_date=vo_adj_split_date,
            calendar=calendar,
            instru_info_table=instru_info_table,
            verbose=False
        )
    elif switch in ["MBR"]:
        from DbMember import cal_member

        cal_member(
            # db_save_dir=futures_by_instrument_dir, db_save_name=instrument_member_db_name, instrument_ids=concerned_universe,
            db_save_dir=futures_by_instrument_dir, db_save_name=instrument_member_db_name, instrument_ids=["Y.DCE", "CF.CZC", "RB.SHF", "IC.CFE"],
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            src_db_structure_path=futures_fundamental_structure_path,
            src_db_name=futures_fundamental_db_name,
            src_tab_name=src_tab_name_member,
            src_db_dir=futures_fundamental_dir,
            vo_adj_split_date=vo_adj_split_date,
            calendar=calendar,
            verbose=False
        )
    else:
        print(f"... switch = {switch} is not a legal option, please check again.")
