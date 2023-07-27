"""
created @ 2023-07-27
0.  to summary volume, amount, and oi information by Instrument
1.  use argument t_src_tab_name to choose the source of md,
    {
        "CTable": "WDS",
        "CTable2": "TSDB",
    }
"""

import pandas as pd
from skyrim.whiterun import CCalendar, CInstrumentInfoTable
from skyrim.falkreath import CTable
from DbByInstrument import CDbByInstrumentSQL


class CDbByInstrumentSQLVolume(CDbByInstrumentSQL):
    def __init__(self, db_save_dir: str, db_save_name: str, instrument_ids: list[str], run_mode: str,
                 src_db_structure_path: str, src_db_name: str, src_tab_name: str, src_db_dir: str,
                 vo_adj_split_date: str,
                 calendar: CCalendar, instru_info_table: CInstrumentInfoTable, verbose: bool):
        self.m_vo_adj_split_date = vo_adj_split_date  # "20200101"
        self.m_instru_info_table = instru_info_table

        # init tables
        tables = [CTable(t_table_struct={
            "table_name": instrument_id.replace(".", "_"),
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {
                "volume": "INTEGER",
                "amount": "REAL",
                "oi": "INTEGER",
                "sizeClose": "REAL",
                "sizeSettle": "REAL",
            },
        }) for instrument_id in instrument_ids]
        super().__init__(db_save_dir=db_save_dir, db_save_name=db_save_name, tables=tables, run_mode=run_mode,
                         src_db_structure_path=src_db_structure_path, src_db_name=src_db_name,
                         src_tab_name=src_tab_name, src_db_dir=src_db_dir,
                         calendar=calendar, verbose=verbose)

    def __update_volume_like_data(self, instrument_id: str, bgn_date: str, stp_date: str):
        instrument, exchange = instrument_id.split(".")
        contract_multiplier = self.m_instru_info_table.get_multiplier(instrument_id)

        # --- load historical data
        db_reader = self.get_src_reader()
        md_df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
            ("instrument", "=", instrument),
        ], t_value_columns=["trade_date", "loc_id", "close", "settle", "volume", "amount", "oi"],
        ).rename(mapper={"loc_id": "contract"}, axis=1)
        db_reader.close()

        # --- fillna
        md_df[["volume", "amount", "oi"]] = md_df[["volume", "amount", "oi"]].fillna(0)
        md_df["sizeClose"] = md_df["close"] * md_df["oi"] * contract_multiplier
        md_df["sizeSettle"] = md_df["settle"] * md_df["oi"] * contract_multiplier
        md_df[["sizeClose", "sizeSettle"]] = md_df[["sizeClose", "sizeSettle"]].fillna(0)

        # --- update md
        volume_df = pd.pivot_table(data=md_df, values=["volume", "amount", "oi", "sizeClose", "sizeSettle"], index="trade_date", aggfunc=sum)
        vo_adj_ratio = 1 if exchange in ["CFE"] else [2 if trade_date < self.m_vo_adj_split_date else 1 for trade_date in volume_df.index]
        volume_df = volume_df.div(vo_adj_ratio, axis="index")
        volume_df.reset_index(inplace=True)

        # --- column selection
        return volume_df[["trade_date", "volume", "amount", "oi", "sizeClose", "sizeSettle"]]

    def _get_update_data_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        if self._check_continuity(instrument_id, run_mode, bgn_date):
            update_df = self.__update_volume_like_data(instrument_id, bgn_date, stp_date)
            instru_tab_name = instrument_id.replace(".", "_")
            self._save(update_df=update_df, using_index=False, table_name=instru_tab_name)
        return 0

    def _print_tips(self):
        print("... volume, amount, oi, sizeClose and sizeSettle calculated.")
        return 0
