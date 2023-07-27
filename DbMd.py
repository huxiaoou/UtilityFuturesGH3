"""
created @ 2023-04-18
0.  this script will reorganize the close and open data from by-date to by-instrument
1.  stp date will work NEITHER in mode "O" NOR in mode "A"
2.  the results are a huge table with SIZE = T * N, where
    T ~ O(252 * 20)
    N ~ O(12 * 20)
3.  use argument t_src_tab_name to choose the source of md,
    {
        "CTable": "WDS",
        "CTable2": "TSDB",
    }
"""

import os
import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.whiterun import CCalendar
from DbByInstrument import CDbByInstrumentCSV


class CDbByInstrumentCSVMd(CDbByInstrumentCSV):
    def __update_md(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        instrument, exchange = instrument_id.split(".")
        db_reader = self.get_src_reader()
        md_df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
            ("instrument", "=", instrument),
        ], t_value_columns=["trade_date", "loc_id"] + self.m_price_types,
        ).rename(mapper={"loc_id": "contract"}, axis=1)
        db_reader.close()

        for price_type in self.m_price_types:
            price_df = pd.pivot_table(data=md_df, values=price_type, index="trade_date", columns="contract", dropna=False)
            price_df = price_df.reset_index().rename(mapper={"index": "trade_date"}, axis=1)

            # --- set destination
            price_file = self.m_price_file_prototype.format(instrument_id, price_type)
            price_path = os.path.join(self.m_md_by_instru_dir, price_file)
            if run_mode.upper() in ["A", "APPEND"]:
                old_price_df = pd.read_csv(price_path, dtype={"trade_date": str})
                new_price_df = pd.concat([old_price_df, price_df])
            else:
                new_price_df = price_df
            new_price_df.to_csv(price_path, float_format="%.2f", index=False)
        return 0

    def get_update_data_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        if self.check_continuity(instrument_id, run_mode, bgn_date):
            self.__update_md(instrument_id, run_mode, bgn_date, stp_date)
        return 0


def cal_md(
        proc_num: int,
        md_by_instru_dir: str, price_types: list[str],
        instrument_ids: list[str],
        run_mode: str, bgn_date: str, stp_date: str,
        futures_md_structure_path: str, futures_md_db_name: str, src_tab_name: str, futures_md_dir: str,
        calendar: CCalendar):
    db_by_instrument = CDbByInstrumentCSVMd(
        md_by_instru_dir=md_by_instru_dir,
        price_types=price_types,
        src_db_structure_path=futures_md_structure_path,
        src_db_name=futures_md_db_name,
        src_tab_name=src_tab_name,
        src_db_dir=futures_md_dir,
        calendar=calendar,
    )

    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for instrument_id in instrument_ids:
        pool.apply_async(db_by_instrument.get_update_data_by_instrument, args=(instrument_id, run_mode, bgn_date, stp_date))
    pool.close()
    pool.join()
    db_by_instrument.close()
    t1 = dt.datetime.now()
    print("... market data calculated")
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
