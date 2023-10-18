import argparse
import datetime as dt


def parse_args():
    args_parser = argparse.ArgumentParser(description="Entry point of this project", formatter_class=argparse.RawTextHelpFormatter)
    args_parser.add_argument("-w", "--switch", type=str, choices=("mm", "mr", "md", "fd", "vol", "mbr"),
                             help="""use this to decide which parts to run, available options = {
        'mm': major_minor,
        'mr': major_return,
        'md': market_data,
        'fd': fundamental, no run mode needed
        'vol': volume sum by instrument,
        'mbr': member information, no data source type needed, only from wind
        }""")
    args_parser.add_argument("-p", "--process", type=int, default=5, help="""number of process to be called when calculating, default = 5""")
    args_parser.add_argument("-m", "--mode", type=str, choices=("o", "a"), help="""run mode, available options = {'o', 'a'}""")
    args_parser.add_argument("-b", "--bgn", type=str, help="""begin date, suggested = '20120101'. """)
    args_parser.add_argument("-s", "--stp", type=str, help="""stop date, not included.""")
    args_parser.add_argument("-r", "--source", type=str, choices=("wds", "tsdb"), help="""type of source data""")

    args = args_parser.parse_args()
    _switch = args.switch.upper()
    _proc_num = args.process
    _run_mode = None if _switch in ["FD"] else args.mode.upper()
    _bgn_date, _stp_date = args.bgn, args.stp
    _stp_date = (dt.datetime.strptime(_bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d") if _stp_date is None else _stp_date
    _src_type = None if _switch in ["MBR"] else args.source.upper()
    return _switch, _proc_num, _run_mode, _bgn_date, _stp_date, _src_type


if __name__ == "__main__":
    from project_setup import (global_config, calendar_path, futures_instru_info_path,
                               futures_dir, db_structs, futures_md_wds_db_name, futures_md_tsdb_db_name,
                               futures_by_instrument_dir, )
    from skyrim.whiterun import CCalendar, CInstrumentInfoTable

    switch, proc_num, run_mode, bgn_date, stp_date, src_type = parse_args()

    # manual config
    volume_mov_ave_n_config, volume_mov_ave_n_default = {"IH.CFE": 1, "IF.CFE": 1, "IC.CFE": 1, "IM.CFE": 1,
                                                         "TS.CFE": 1, "T.CFE": 1, "TF.CFE": 1, "TL.CFE": 1, }, 3
    major_return_price_type = "close"
    vo_adj_split_date = "20200101"
    price_types = ["open", "close", "settle"]

    # shared calendar and instru_info_table
    calendar = CCalendar(calendar_path)
    instru_info_table_w = CInstrumentInfoTable(t_path=futures_instru_info_path, t_type="CSV", t_index_label="windCode")  # for switch == "VOL"
    instru_info_table_i = CInstrumentInfoTable(t_path=futures_instru_info_path, t_type="CSV", t_index_label="instrumentId")  # others
    concerned_universe = instru_info_table_w.get_universe()

    # main
    if switch in ["MM"]:
        from project_setup import major_minor_db_name
        from DbMajorMinor import CDbByInstrumentSQLMajorMinor

        src_db_name = futures_md_wds_db_name if src_type == "WDS" else futures_md_tsdb_db_name
        db_by_instrument = CDbByInstrumentSQLMajorMinor(
            instrument_ids=concerned_universe,
            # instrument_ids=["SH.CZC", "PX.CZC"], # use this with run_mode = "o" to add  new instruments
            volume_mov_ave_n_config=volume_mov_ave_n_config,
            volume_mov_ave_n_default=volume_mov_ave_n_default,
            db_save_dir=futures_by_instrument_dir, db_save_name=major_minor_db_name,
            run_mode=run_mode, verbose=False,
            proc_num=proc_num, src_db_name=src_db_name, src_tab_name="CTable", src_db_dir=futures_dir,
            src_db_structs=db_structs, calendar=calendar
        )
        db_by_instrument.main_loop(instrument_ids=concerned_universe, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date)
    elif switch in ["MR"]:
        from project_setup import major_return_db_name, major_minor_db_name
        from DbMajorReturn import CDbByInstrumentSQLMajorReturn

        src_db_name = futures_md_wds_db_name if src_type == "WDS" else futures_md_tsdb_db_name
        db_by_instrument = CDbByInstrumentSQLMajorReturn(
            instrument_ids=concerned_universe, major_return_price_type=major_return_price_type, vo_adj_split_date=vo_adj_split_date,
            major_minor_lib_dir=futures_by_instrument_dir, major_minor_lib_name=major_minor_db_name,
            db_save_dir=futures_by_instrument_dir, db_save_name=major_return_db_name,
            run_mode=run_mode, verbose=False,
            proc_num=proc_num, src_db_name=src_db_name, src_tab_name="CTable", src_db_dir=futures_dir,
            src_db_structs=db_structs, calendar=calendar,
        )
        db_by_instrument.main_loop(instrument_ids=concerned_universe, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date)
    elif switch in ["MD"]:
        from project_setup import by_instru_md_dir
        from DbMd import CDbByInstrumentCSVMd

        src_db_name = futures_md_wds_db_name if src_type == "WDS" else futures_md_tsdb_db_name
        db_by_instrument = CDbByInstrumentCSVMd(
            by_instru_md_dir=by_instru_md_dir, price_types=price_types,
            proc_num=proc_num, src_db_name=src_db_name, src_tab_name="CTable", src_db_dir=futures_dir,
            src_db_structs=db_structs, calendar=calendar,
        )
        db_by_instrument.main_loop(instrument_ids=concerned_universe, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date)
    elif switch in ["FD"]:
        if src_type == "TSDB":
            from project_setup import by_instru_fd_dir
            from TSDBTranslator2.translator import CTSDBReader
            from CalFundFromTSDB import update_fundamental_by_instrument

            tsdb_reader = CTSDBReader(t_tsdb_path=global_config["TSDB"]["path"]["private"])
            update_fundamental_by_instrument(
                bgn_date=bgn_date, stp_date=stp_date,
                tsdb_reader=tsdb_reader,
                calendar=calendar,
                instru_info_table=instru_info_table_i,
                by_instru_fd_dir=by_instru_fd_dir,
            )
        elif src_type == "WDS":
            from project_setup import futures_fundamental_db_name, by_instru_fd_dir
            from skyrim.falkreath import CManagerLibReader
            from CalFundFromSQL import update_fundamental_by_instrument_from_sql

            sql_reader = CManagerLibReader(t_db_save_dir=futures_dir, t_db_name=futures_fundamental_db_name)
            update_fundamental_by_instrument_from_sql(
                bgn_date=bgn_date, stp_date=stp_date,
                sql_reader=sql_reader,
                calendar=calendar,
                by_instru_fd_dir=by_instru_fd_dir,
            )
    elif switch in ["VOL"]:
        from project_setup import instrument_volume_db_name
        from DbVolume import CDbByInstrumentSQLVolume

        src_db_name = futures_md_wds_db_name if src_type == "WDS" else futures_md_tsdb_db_name
        db_by_instrument = CDbByInstrumentSQLVolume(
            instrument_ids=concerned_universe,
            vo_adj_split_date=vo_adj_split_date, instru_info_table=instru_info_table_w,
            db_save_dir=futures_by_instrument_dir, db_save_name=instrument_volume_db_name,
            run_mode=run_mode, verbose=False,
            proc_num=proc_num, src_db_name=src_db_name, src_tab_name="CTable", src_db_dir=futures_dir,
            src_db_structs=db_structs, calendar=calendar,
        )
        db_by_instrument.main_loop(instrument_ids=concerned_universe, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date)
    elif switch in ["MBR"]:
        from project_setup import instrument_member_db_name, futures_positions_c_db_name, futures_positions_e_db_name
        from DbMember import CDbByInstrumentSQLMember

        universe_c = [z for z in concerned_universe if z.split(".")[1] != "CFE"]
        universe_e = [z for z in concerned_universe if z.split(".")[1] == "CFE"]

        db_by_instrument = CDbByInstrumentSQLMember(
            instrument_ids=universe_c, exception_ids=["WR.SHF"],
            db_save_dir=futures_by_instrument_dir, db_save_name=instrument_member_db_name,
            run_mode=run_mode, verbose=False,
            proc_num=proc_num, src_db_name=futures_positions_c_db_name, src_tab_name="CTable", src_db_dir=futures_dir,
            src_db_structs=db_structs, calendar=calendar,
        )
        db_by_instrument.main_loop(instrument_ids=universe_c, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date)

        db_by_instrument = CDbByInstrumentSQLMember(
            instrument_ids=universe_e, exception_ids=[],
            db_save_dir=futures_by_instrument_dir, db_save_name=instrument_member_db_name,
            run_mode=run_mode, verbose=False,
            proc_num=proc_num, src_db_name=futures_positions_e_db_name, src_tab_name="CTable", src_db_dir=futures_dir,
            src_db_structs=db_structs, calendar=calendar,
        )
        db_by_instrument.main_loop(instrument_ids=universe_e, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date)

    else:
        print(f"... switch = {switch} is not a legal option, please check again.")
