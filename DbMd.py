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
import pandas as pd
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

    def _get_update_data_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        if self._check_continuity(instrument_id, run_mode, bgn_date):
            self.__update_md(instrument_id, run_mode, bgn_date, stp_date)
        return 0

    def _print_tips(self):
        print("... market data calculated")
        return 0
