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
from skyrim.whiterun import SetFontGreen
from skyrim.falkreath import CTable
from DbByInstrument import CDbByInstrumentSQL


class CDbByInstrumentSQLMajorMinor(CDbByInstrumentSQL):
    def __init__(self, instrument_ids: list[str], volume_mov_ave_n_config: dict[str, int], volume_mov_ave_n_default: int, **kwargs):
        # unique member
        self.m_volume_mov_ave_n_config: dict[str, int] = volume_mov_ave_n_config
        self.m_volume_mov_ave_n_default: int = volume_mov_ave_n_default

        # init tables
        tables = [CTable(t_table_struct={
            "table_name": instrument_id.replace(".", "_"),
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"n_contract": "TEXT", "d_contract": "TEXT"},
        }) for instrument_id in instrument_ids]
        super().__init__(tables=tables, **kwargs)

    @staticmethod
    def __parse_n_contract_and_d_contract(vol_srs: pd.Series, instrument: str, exchange: str):
        _trade_date = vol_srs.name
        _trade_month = _trade_date[2:6]  # format = "YYMM"
        _trade_month_contract = instrument + _trade_month + "." + exchange
        if instrument.upper() in ["IC", "IF", "IH", "IM", "TS", "TF", "T", "TL"]:
            n_contract = vol_srs[vol_srs.index >= _trade_month_contract].idxmax()
        else:
            n_contract = vol_srs[vol_srs.index > _trade_month_contract].idxmax()

        d_srs = vol_srs[vol_srs.index > n_contract]
        if d_srs.empty:
            d_contract = vol_srs[vol_srs.index < n_contract].idxmax()
            n_contract, d_contract = d_contract, n_contract
        else:
            d_contract = d_srs.idxmax()
        return n_contract, d_contract

    def __update_major_minor(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        instrument, exchange = instrument_id.split(".")
        volume_mov_ave_n = self.m_volume_mov_ave_n_config.get(instrument_id, self.m_volume_mov_ave_n_default)

        iter_dates = self.calendar.get_iter_list(bgn_date, stp_date, True)
        base_date = self.calendar.get_next_date(iter_dates[0], -volume_mov_ave_n + 1)

        # --- load historical data
        db_reader = self._get_src_reader()
        md_df = db_reader.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", base_date),
                ("trade_date", "<", stp_date),
                ("instrument", "=", instrument),
                ("exchange", "=", exchange),
            ],
            t_value_columns=["trade_date", "loc_id", "volume"],
        ).rename(mapper={"loc_id": "contract"}, axis=1)
        db_reader.close()

        # --- pivot
        if not md_df.empty:
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
        else:
            print(f"... Warning! There is no data for {SetFontGreen(instrument_id)}, make sure it's expected")
            major_minor_df = pd.DataFrame()
        return major_minor_df

    def _get_update_data_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str) -> tuple[str, list[tuple[str, pd.DataFrame]]] | None:
        if self._check_continuity(instrument_id, run_mode, bgn_date) == 0:
            update_df = self.__update_major_minor(instrument_id, run_mode, bgn_date, stp_date)
            return instrument_id, [("major_minor", update_df)]
        return None

    def _print_tips(self):
        print(f"... {SetFontGreen('major and minor contracts')} calculated")
        return 0
