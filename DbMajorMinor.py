"""
created @ 2023-07-24
0.  this script only decide which contract is major and which is minor on T day
    so if we just want always trade major contract, the results will help
1.  set volume delay from 1 to 2, so that major return is executable
2.  use argument t_src_tab_name to choose the source of md,
    {
        "CTable": "WDS",
        "CTable2": "TSDB",
    }
"""

import pandas as pd
from skyrim.whiterun import CCalendar, SetFontGreen
from skyrim.falkreath import CTable
from DbByInstrument import CDbByInstrumentSQL


class CDbByInstrumentSQLMajorMinor(CDbByInstrumentSQL):
    def __init__(self, proc_num: int, db_save_dir: str, db_save_name: str, instrument_ids: list[str], run_mode: str,
                 src_db_structure_path: str, src_db_name: str, src_tab_name: str, src_db_dir: str,
                 volume_mov_ave_n_config: dict[str, int], volume_mov_ave_n_default: int,
                 calendar: CCalendar, verbose: bool):
        # unique member
        self.m_volume_mov_ave_n_config: dict[str, int] = volume_mov_ave_n_config
        self.m_volume_mov_ave_n_default: int = volume_mov_ave_n_default

        # init tables
        tables = [CTable(t_table_struct={
            "table_name": instrument_id.replace(".", "_"),
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"n_contract": "TEXT", "d_contract": "TEXT"},
        }) for instrument_id in instrument_ids]
        super().__init__(proc_num=proc_num, db_save_dir=db_save_dir, db_save_name=db_save_name, tables=tables, run_mode=run_mode,
                         src_db_structure_path=src_db_structure_path, src_db_name=src_db_name,
                         src_tab_name=src_tab_name, src_db_dir=src_db_dir,
                         calendar=calendar, verbose=verbose)

    @staticmethod
    def __parse_n_contract_and_d_contract(vol_srs: pd.Series, instrument: str, exchange: str):
        _trade_date = vol_srs.name
        _trade_month = _trade_date[2:6]  # format = "YYMM"
        _trade_month_contract = instrument + _trade_month + "." + exchange
        if instrument.upper() in ["IC", "IF", "IH", "IM", "TS", "TF", "T", "TL"]:
            n_contract = vol_srs[vol_srs.index >= _trade_month_contract].idxmax()
        else:
            n_contract = vol_srs[vol_srs.index > _trade_month_contract].idxmax()
        d_contract = vol_srs[vol_srs.index > n_contract].idxmax()
        return n_contract, d_contract

    def __update_major_minor(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        instrument, exchange = instrument_id.split(".")
        volume_mov_ave_n = self.m_volume_mov_ave_n_config.get(instrument_id, self.m_volume_mov_ave_n_default)

        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        base_date = self.calendar.get_next_date(iter_dates[0], -volume_mov_ave_n + 1)

        # --- load historical data
        db_reader = self._get_src_reader()
        md_df = db_reader.read_by_instrument_and_time_window(
            t_instrument=instrument,
            t_value_columns=["trade_date", "loc_id", "volume"],
            t_bgn_date=base_date,
            t_stp_date=stp_date,
        ).rename(mapper={"loc_id": "contract"}, axis=1)
        db_reader.close()

        # --- pivot
        pivot_volume_df = pd.pivot_table(data=md_df, values="volume", index="trade_date", columns="contract").fillna(0)
        pivot_volume_df_sorted = pivot_volume_df.sort_index(axis=1).sort_index(axis=0)
        volume_mov_aver_df = pivot_volume_df_sorted.rolling(window=volume_mov_ave_n).mean()
        volume_mov_aver_df = volume_mov_aver_df.loc[volume_mov_aver_df.index >= bgn_date]
        if run_mode in ["O", "OVERWRITE"]:
            volume_mov_aver_df = volume_mov_aver_df.fillna(method="bfill")  # for first few rows

        # main loop to get major and minor contracts
        volume_mov_aver_df["n_contract"], volume_mov_aver_df["d_contract"] = \
            zip(*volume_mov_aver_df.apply(self.__parse_n_contract_and_d_contract, args=(instrument, exchange), axis=1))
        major_minor_df = volume_mov_aver_df[["n_contract", "d_contract"]].reset_index()
        return major_minor_df

    def _get_update_data_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        if self._check_continuity(instrument_id, run_mode, bgn_date) == 0:
            update_df = self.__update_major_minor(instrument_id, run_mode, bgn_date, stp_date)
            instru_tab_name = instrument_id.replace(".", "_")
            self._save(instrument_id=instrument_id, update_df=update_df, using_index=False, table_name=instru_tab_name)
        return 0

    def _print_tips(self):
        print(f"... {SetFontGreen('major and minor contracts')} calculated")
        return 0
