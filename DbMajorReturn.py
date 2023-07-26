"""
created @ 2023-04-17
0.  this script is based on the results of cal_major_minor
1.  the major contract of T-day is decided by the information before and at T day, so the major return of T
    is defined as
        major return [T]  = close[T] / close[T-1]
    this return is almost executable in reality because AT/BEFORE the close time point of T-1, we 
    almost do know which contract would be major contract at T, and the results would almost be the
    same as the results AFTER the CLOSE time point.
2.  this result is tradable and can be used as a market index replacement.
3.  the output of this script may be replaced by MARKET INDEX in some scenarios.
4.  use argument t_src_tab_name to choose the source of md,
    {
        "CTable": "WDS",
        "CTable2": "TSDB",
    }
"""

import datetime as dt
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CTable, CManagerLibReader
from DbByInstrument import CDbByInstrumentSQL


class CDbByInstrumentSQLMajorReturn(CDbByInstrumentSQL):
    def __init__(self, db_save_dir: str, db_save_name: str, instrument_ids: list[str], run_mode: str,
                 futures_md_structure_path: str, futures_md_db_name: str, src_tab_name: str, futures_md_dir: str,
                 major_return_price_type: str, vo_adj_split_date: str,
                 major_minor_reader: CManagerLibReader,
                 calendar: CCalendar, verbose: bool):

        self.m_major_return_price_type = major_return_price_type  # "close"
        self.m_vo_adj_split_date = vo_adj_split_date  # "20200101"
        self.major_minor_reader = major_minor_reader

        # init tables
        tables = [CTable(t_table_struct={
            "table_name": instrument_id.replace(".", "_"),
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"n_contract": "TEXT",
                              "prev_close": "REAL",
                              "open": "REAL",
                              "high": "REAL",
                              "low": "REAL",
                              "close": "REAL",
                              "volume": "INTEGER",
                              "amount": "REAL",
                              "oi": "INTEGER",
                              "major_return": "REAL",
                              "instru_idx": "REAL",
                              "openC": "REAL",
                              "highC": "REAL",
                              "lowC": "REAL",
                              "closeC": "REAL",
                              },
        }) for instrument_id in instrument_ids]
        super().__init__(db_save_dir=db_save_dir, db_save_name=db_save_name, tables=tables, run_mode=run_mode,
                         futures_md_structure_path=futures_md_structure_path, futures_md_db_name=futures_md_db_name,
                         src_tab_name=src_tab_name, futures_md_dir=futures_md_dir,
                         calendar=calendar, verbose=verbose)

    @staticmethod
    def cal_major_return(x: pd.Series, t_this_prc_lbl: str, t_prev_prc_lbl: str) -> float:
        res = x[t_this_prc_lbl] / x[t_prev_prc_lbl] - 1
        return 0 if np.isnan(res) else res

    def __update_major_return(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):

        instrument, exchange = instrument_id.split(".")
        this_prc_lbl, prev_prc_lbl = self.m_major_return_price_type, "prev_{}".format(self.m_major_return_price_type)

        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        base_date = self.calendar.get_next_date(iter_dates[0], -1)

        # --- load major table
        major_minor_df = self.major_minor_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "n_contract"], t_using_default_table=False, t_table_name=instrument_id.replace(".", "_"))
        major_minor_df["prev_trade_date"] = major_minor_df["trade_date"].shift(1)
        major_minor_df = major_minor_df.loc[major_minor_df["trade_date"] >= bgn_date]

        # --- set volume adjustment ratio
        if exchange in ["CFE"]:
            major_minor_df["vo_adj_ratio"] = 1
        else:
            major_minor_df["vo_adj_ratio"] = [2 if trade_date < self.m_vo_adj_split_date else 1 for trade_date in major_minor_df["trade_date"]]

        # --- load historical data
        db_reader = self.get_reader()
        md_df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", base_date),
            ("trade_date", "<", stp_date),
            ("instrument", "=", instrument),
        ], t_value_columns=["trade_date", "loc_id", "open", "high", "low", "close", "volume", "amount", "oi"],
        ).rename(mapper={"loc_id": "contract"}, axis=1)
        db_reader.close()

        # --- fillna
        md_df[["open", "high", "low", "close"]] = md_df[["open", "high", "low", "close"]].fillna(np.nan)
        md_df[["volume", "amount", "oi"]] = md_df[["volume", "amount", "oi"]].fillna(0)

        # --- update md
        major_return_df = pd.merge(
            left=major_minor_df, right=md_df,
            left_on=["trade_date", "n_contract"],
            right_on=["trade_date", "contract"],
            how="left"
        )

        # --- update prev close
        major_return_df = pd.merge(
            left=major_return_df, right=md_df[["trade_date", "contract", self.m_major_return_price_type]],
            left_on=["prev_trade_date", "n_contract"],
            right_on=["trade_date", "contract"],
            how="left", suffixes=("", "_prev")
        ).rename(mapper={self.m_major_return_price_type + "_prev": prev_prc_lbl}, axis=1)

        # --- adjust volume amount openInterest
        major_return_df[["volume", "amount", "oi"]] = major_return_df[["volume", "amount", "oi"]].div(major_return_df["vo_adj_ratio"], axis="index").fillna(0)

        # --- major return
        major_return_df["major_return"] = major_return_df[[this_prc_lbl, prev_prc_lbl]].apply(
            self.cal_major_return, t_this_prc_lbl=this_prc_lbl, t_prev_prc_lbl=prev_prc_lbl, axis=1)

        # --- instru idx
        if run_mode in ["O", "OVERWRITE"]:
            major_return_df[prev_prc_lbl].fillna(method="bfill", inplace=True)
            base_val_instru_idx = 1
            base_val_close_price = major_return_df["close"].dropna().iloc[0]
        else:
            base_date_df = self.m_by_instru_db.read_by_date(t_trade_date=base_date, t_value_columns=["instru_idx", "closeC"],
                                                            t_using_default_table=False, t_table_name=instrument_id.replace(".", "_"))
            base_val_instru_idx = base_date_df["instru_idx"].iloc[-1]
            base_val_close_price = base_date_df["closeC"].iloc[-1]
        major_return_df["instru_idx"] = (major_return_df["major_return"] + 1).cumprod() * base_val_instru_idx
        major_return_df["closeC"] = (major_return_df["major_return"] + 1).cumprod() * base_val_close_price

        # --- recovered price
        base_srs_close_price = major_return_df["closeC"].shift(1).fillna(base_val_close_price)
        major_return_df[["openC", "highC", "lowC"]] = major_return_df[["open", "high", "low"]].div(
            major_return_df["prev_close"], axis="index").multiply(base_srs_close_price, axis="index")

        # --- column selection
        return major_return_df[[
            "trade_date", "n_contract",
            prev_prc_lbl, "open", "high", "low", "close", "volume", "amount", "oi",
            "major_return", "instru_idx",
            "openC", "highC", "lowC", "closeC"
        ]]

    def get_update_data_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        if self.check_continuity(instrument_id, run_mode, bgn_date):
            update_df = self.__update_major_return(instrument_id, run_mode, bgn_date, stp_date)
            instru_tab_name = instrument_id.replace(".", "_")
            self.save(update_df=update_df, using_index=False, table_name=instru_tab_name)
        return 0


def cal_major_return(
        db_save_dir: str, db_save_name: str, instrument_ids: list[str],
        run_mode: str, bgn_date: str, stp_date: str,
        futures_md_structure_path: str, futures_md_db_name: str, src_tab_name: str, futures_md_dir: str,
        major_return_price_type: str, vo_adj_split_date: str,
        major_minor_dir: str, major_minor_db_name: str,
        calendar: CCalendar, verbose: bool,
):
    major_minor_reader = CManagerLibReader(major_minor_dir, major_minor_db_name)
    db_by_instrument = CDbByInstrumentSQLMajorReturn(
        db_save_dir=db_save_dir, db_save_name=db_save_name, instrument_ids=instrument_ids,
        run_mode=run_mode,
        futures_md_structure_path=futures_md_structure_path,
        futures_md_db_name=futures_md_db_name,
        src_tab_name=src_tab_name,
        futures_md_dir=futures_md_dir,
        major_return_price_type=major_return_price_type,
        vo_adj_split_date=vo_adj_split_date,
        major_minor_reader=major_minor_reader,
        calendar=calendar,
        verbose=verbose,
    )

    t0 = dt.datetime.now()
    for instrument_id in instrument_ids:
        db_by_instrument.get_update_data_by_instrument(instrument_id, run_mode, bgn_date, stp_date)
    db_by_instrument.close()
    major_minor_reader.close()
    t1 = dt.datetime.now()
    print("... major return calculated".format((t1 - t0).total_seconds()))
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
