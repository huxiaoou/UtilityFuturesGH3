"""
created @ 2023-07-27
0.  to summary volume, long position and short position of members by Instrument
"""

import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CTable
from DbByInstrument import CDbByInstrumentSQL


class CDbByInstrumentSQLMember(CDbByInstrumentSQL):
    def __init__(self, db_save_dir: str, db_save_name: str, instrument_ids: list[str], run_mode: str,
                 src_db_structure_path: str, src_db_name: str, src_tab_name: str, src_db_dir: str,
                 # vo_adj_split_date: str,
                 calendar: CCalendar, verbose: bool):
        # self.m_vo_adj_split_date = vo_adj_split_date  # "20200101"

        # init tables
        tables = [CTable(t_table_struct={
            "table_name": instrument_id.replace(".", "_"),
            "primary_keys": {"trade_date": "TEXT", "member": "TEXT"},
            "value_columns": {
                "volumeSum": "INTEGER",
                "volumeDlt": "INTEGER",
                "lngSum": "INTEGER",
                "lngDlt": "INTEGER",
                "srtSum": "INTEGER",
                "srtDlt": "INTEGER",
            },
        }) for instrument_id in instrument_ids]
        super().__init__(db_save_dir=db_save_dir, db_save_name=db_save_name, tables=tables, run_mode=run_mode,
                         src_db_structure_path=src_db_structure_path, src_db_name=src_db_name,
                         src_tab_name=src_tab_name, src_db_dir=src_db_dir,
                         calendar=calendar, verbose=verbose)

    def __update_member_data(self, instrument_id: str, bgn_date: str, stp_date: str):
        instrument, exchange = instrument_id.split(".")
        if exchange in ["CFE"]:
            return pd.DataFrame()

        # --- load historical data
        db_reader = self._get_src_reader()
        md_df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
            ("instrument", "=", instrument),
        ], t_value_columns=["trade_date", "loc_id", "member", "rnk_type", "pos_qty", "pos_dlt"],
        ).rename(mapper={"loc_id": "contract"}, axis=1)
        db_reader.close()

        # --- transform
        member_df = pd.pivot_table(data=md_df, values=["pos_qty", "pos_dlt"], index=["trade_date", "member"], columns=["rnk_type"], aggfunc=sum).fillna(0)
        # vo_adj_ratio = 1 if exchange in ["CFE"] else [2 if trade_date < self.m_vo_adj_split_date else 1 for trade_date, _ in member_df.index]
        # member_df = member_df.div(vo_adj_ratio, axis="index")
        member_df.reset_index(inplace=True)

        if instrument_id in ["Y.DCE", "CF.CZC", "RB.SHF"]:
            test_df = pd.pivot_table(data=md_df, values=["pos_qty", "pos_dlt"], index=["trade_date"], columns=["rnk_type"], aggfunc=sum).fillna(0)
            print(instrument_id, test_df.head(20))
        print(f"... member position information of {instrument_id:>6s} are aggregated")

        # --- column selection
        return member_df[[
            ("trade_date", ""),
            ("member", ""),
            ("pos_qty", "1"),
            ("pos_dlt", "1"),
            ("pos_qty", "2"),
            ("pos_dlt", "2"),
            ("pos_qty", "3"),
            ("pos_dlt", "3"),
        ]]

    def _get_update_data_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        if self._check_continuity(instrument_id, run_mode, bgn_date):
            update_df = self.__update_member_data(instrument_id, bgn_date, stp_date)
            instru_tab_name = instrument_id.replace(".", "_")
            self._save(update_df=update_df, using_index=False, table_name=instru_tab_name)
        return 0

    def _print_tips(self):
        print("... member information are calculated.")
        return 0
