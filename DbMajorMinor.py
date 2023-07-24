"""
created @ 2023-04-17
0.  this script only decide which contract is major and which is minor on T day
    so if we just want always trade major contract, the results will help
1.  set volume delay from 1 to 2, so that major return is executable
2.  use argument t_src_tab_name to choose the source of md,
    {
        "CTable": "WDS",
        "CTable2": "TSDB",
    }
"""

import datetime as dt
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CTable
from DbByInstrument import CDbByInstrument


class CDbByInstrumentMajorMinor(CDbByInstrument):
    def __init__(self, db_save_dir: str, db_save_name: str, instrument_ids: list[str], run_mode: str,
                 src_tab_name: str, futures_md_structure_path: str, futures_md_db_name: str, futures_md_dir: str,
                 volume_mov_ave_n_config: dict[str, int], volume_mov_ave_n_default: int,
                 calendar: CCalendar, verbose: bool = False):
        # unique member
        self.m_volume_mov_ave_n_config: dict[str, int] = volume_mov_ave_n_config
        self.m_volume_mov_ave_n_default: int = volume_mov_ave_n_default

        # init tables
        tables = [CTable(t_table_struct={
            "table_name": instrument_id.replace(".", "_"),
            "primary_keys": {"trade_date": "TXT"},
            "value_columns": {"n_contract": "TXT", "d_contract": "TXT"},
        }) for instrument_id in instrument_ids]
        super().__init__(db_save_dir, db_save_name, tables, run_mode,
                         src_tab_name, futures_md_structure_path, futures_md_db_name, futures_md_dir,
                         calendar, verbose)

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
        volume_mov_ave_n = self.m_volume_mov_ave_n_config.get(instrument_id, 3)

        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        base_date = self.calendar.get_next_date(iter_dates[0], -volume_mov_ave_n + 1)

        # --- load historical data
        md_df = self.md_db.read_by_instrument_and_time_window(
            t_instrument=instrument,
            t_value_columns=["trade_date", "loc_id", "volume"],
            t_bgn_date=base_date,
            t_stp_date=stp_date,
        ).rename(mapper={"loc_id": "contract"}, axis=1)

        # --- pivot
        pivot_volume_df = pd.pivot_table(data=md_df, values="volume", index="trade_date", columns="contract").fillna(0)
        pivot_volume_df_sorted = pivot_volume_df.sort_index(axis=1).sort_index(axis=0)
        volume_mov_aver_df = pivot_volume_df_sorted.rolling(window=volume_mov_ave_n).mean()
        if run_mode in ["O", "OVERWRITE"]:
            volume_mov_aver_df = volume_mov_aver_df.fillna(method="bfill")  # for first few rows
        else:
            # drop first few rows, where the moving average volume is not correct
            volume_mov_aver_df = volume_mov_aver_df.tail(len(volume_mov_aver_df) - volume_mov_ave_n + 1)

        # main loop to get major and minor contracts
        volume_mov_aver_df["n_contract"], volume_mov_aver_df["d_contract"] = \
            zip(*volume_mov_aver_df.apply(self.__parse_n_contract_and_d_contract, args=(instrument, exchange), axis=1))
        major_minor_df = volume_mov_aver_df[["n_contract", "d_contract"]].reset_index()
        filter_dates = (major_minor_df["trade_date"] >= bgn_date) & (major_minor_df["trade_date"] < stp_date)
        major_minor_df = major_minor_df.loc[filter_dates]
        return major_minor_df

    def get_update_df_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        update_df = self.__update_major_minor(instrument_id, run_mode, bgn_date, stp_date)
        instru_tab_name = instrument_id.replace(".", "_")
        self.save(table_name=instru_tab_name, update_df=update_df)
        return 0


def cal_major_minor_mp(instrument_ids: list[str],
                       db_save_dir: str, db_save_name: str,
                       run_mode: str, bgn_date: str, stp_date: str,
                       src_tab_name: str, futures_md_structure_path: str, futures_md_db_name: str, futures_md_dir: str,
                       calendar_path: str,
                       volume_mov_ave_n_config: dict[str, int], volume_mov_ave_n_default: int,
                       verbose: bool,
                       ):
    calendar = CCalendar(calendar_path)

    db_by_instrument = CDbByInstrumentMajorMinor(
        db_save_dir=db_save_dir, db_save_name=db_save_name,
        instrument_ids=instrument_ids,
        run_mode=run_mode,
        src_tab_name=src_tab_name,
        futures_md_structure_path=futures_md_structure_path,
        futures_md_db_name=futures_md_db_name,
        futures_md_dir=futures_md_dir,
        volume_mov_ave_n_config=volume_mov_ave_n_config,
        volume_mov_ave_n_default=volume_mov_ave_n_default,
        calendar=calendar,
        verbose=verbose,
    )

    t0 = dt.datetime.now()
    for instrument_id in instrument_ids:
        db_by_instrument.get_update_df_by_instrument(instrument_id, run_mode, bgn_date, stp_date)
    db_by_instrument.close()
    t1 = dt.datetime.now()
    print("... major and minor contracts calculated".format((t1 - t0).total_seconds()))
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
