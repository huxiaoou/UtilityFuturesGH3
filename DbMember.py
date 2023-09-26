"""
created @ 2023-07-27
0.  to summary volume, long position and short position of members by Instrument
"""

import datetime
import pandas as pd
from skyrim.whiterun import SetFontGreen
from skyrim.falkreath import CTable
from DbByInstrument import CDbByInstrumentSQL


class CDbByInstrumentSQLMember(CDbByInstrumentSQL):
    def __init__(self, instrument_ids: list[str], **kwargs):

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
        super().__init__(tables=tables, **kwargs)

    def __update_member_data(self, instrument_id: str, bgn_date: str, stp_date: str):
        instrument, exchange = instrument_id.split(".")

        # --- load historical data
        db_reader = self._get_src_reader()
        md_df = db_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
            ("instrument", "=", instrument),
        ], t_value_columns=["trade_date", "loc_id", "member", "rnk_type", "pos_qty", "pos_dlt"],
        ).rename(mapper={"loc_id": "contract"}, axis=1)
        db_reader.close()

        if len(md_df) == 0:
            return pd.DataFrame()

        # --- transform
        # Make sure there is no necessary to adjust volume and position by dividing 2 before "20200101"
        member_df = pd.pivot_table(data=md_df, values=["pos_qty", "pos_dlt"], index=["trade_date", "member"], columns=["rnk_type"], aggfunc=sum).fillna(0)
        member_df.reset_index(inplace=True)

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
        if self._check_continuity(instrument_id, run_mode, bgn_date) in [0, 1]:
            # for some instrument, some days may be omitted, so continuity = 1 is allowed
            update_df = self.__update_member_data(instrument_id, bgn_date, stp_date)
            instru_tab_name = instrument_id.replace(".", "_")
            self._save(instrument_id=instrument_id, update_df=update_df, using_index=False, table_name=instru_tab_name)
            if run_mode in ["O", "OVERWRITE"]:
                print(f"... @ {datetime.datetime.now()} member position information of {SetFontGreen(f'{instrument_id:>6s}')} are aggregated")
        return 0

    def _print_tips(self):
        print(f"... {SetFontGreen('member position information')} are calculated")
        return 0
