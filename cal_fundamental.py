"""
created @ 2023-04-18
0.  this script will reorganize fundamental data from by-date to by-instrument
2.  Since these data are organized by instrument and maybe missing in source, a
    calendar is provided as header to make sure they are all filled.
3.  no argument run_mode is needed, or you can think it is always OVERWRITE
4.  use tsdb as source
"""

from utility_futures_setup import os, dt, pd
from skyrim.whiterun import CCalendar, CInstrumentInfoTable
from TSDBTranslator2.translator import CTSDBReader
from TSDBTranslator2.translator_funs import add_instrument_to_trade_date_tsdb_df


def update_fundamental_by_instrument(
        t_bgn_date: str,
        t_stp_date: str,
        t_tsdb_reader: CTSDBReader,
        t_calendar: CCalendar,
        t_instru_info_table: CInstrumentInfoTable,
        t_fundamental_by_instru_dir: str,
):
    """

    :param t_bgn_date:
    :param t_stp_date:
    :param t_tsdb_reader:
    :param t_calendar:
    :param t_instru_info_table: use instrumentId as index
    :param t_fundamental_by_instru_dir:
    :return:
    """

    fundamental_config = {
        "stock": {
            "values": ["in_stock"],
        },
        "basis": {
            "values": ["basis", "basis_rate"],
        },
    }
    value_columns = []
    for _, cfg in fundamental_config.items():
        value_columns += cfg["values"]
    value_columns_with_table_name = ["huxo.fundamental." + _ for _ in value_columns]

    # --- set trade_date header
    header_df = pd.DataFrame({
        "trade_date": t_calendar.get_iter_list(t_bgn_date, t_stp_date, True),
    })

    # --- load data from TSDB
    end_date = (dt.datetime.strptime(t_stp_date, "%Y%m%d") - dt.timedelta(days=1)).strftime("%Y%m%d")
    tsdb_df = t_tsdb_reader.read_all(t_value_columns=value_columns_with_table_name, tp_beg_date=t_bgn_date, tp_end_date=end_date)
    tsdb_df = tsdb_df.rename(mapper={k: v for k, v in zip(value_columns_with_table_name, value_columns)}, axis=1)

    # --- reformat
    add_instrument_to_trade_date_tsdb_df(t_df=tsdb_df, contract_id="ticker")
    tsdb_df_by_instru = tsdb_df.drop_duplicates(subset=["tp", "instrument"]).copy()
    tsdb_df_by_instru["trade_date"] = tsdb_df_by_instru["tp"].map(lambda z: dt.datetime.fromtimestamp(z / 1e9).strftime("%Y%m%d"))
    # print(tsdb_df_by_instru)

    for instrument, instrument_df in tsdb_df_by_instru.groupby(by="instrument"):
        exchange = t_instru_info_table.get_exchangeId(t_instrument_id=instrument)
        wind_code = instrument.upper() + "." + exchange.upper()[0:3]
        for fundamental_data_type, fundamental_data_type_config in fundamental_config.items():
            fundamental_values = fundamental_data_type_config["values"]
            new_sorted_instrument_df = pd.merge(
                left=header_df, right=instrument_df[["trade_date"] + fundamental_values],
                how="left"
            )
            instrument_file = "{}.{}.csv.gz".format(wind_code, fundamental_data_type.upper())
            instrument_path = os.path.join(t_fundamental_by_instru_dir, instrument_file)
            new_sorted_instrument_df.to_csv(instrument_path, float_format="%.8f", index=False)
            # print("| {} | {:>8s} | {:>8s} | fundamental |".format(dt.datetime.now(), wind_code, fundamental_data_type))
    return 0


if __name__ == "__main__":
    from utility_futures_setup import custom_ts_db_path, calendar_path, futures_instru_info_path
    from utility_futures_setup import fundamental_by_instru_dir

    # test_instrument = "CU.SHF"
    test_instrument = "ZC.CZC"
    tsdb_reader = CTSDBReader(t_tsdb_path=custom_ts_db_path)
    calendar = CCalendar(calendar_path)
    instru_info_table = CInstrumentInfoTable(t_path=futures_instru_info_path, t_type="CSV")
    update_fundamental_by_instrument(
        t_tsdb_reader=tsdb_reader,
        t_calendar=calendar,
        t_instru_info_table=instru_info_table,
        t_bgn_date="20120101",
        t_stp_date="20230419",
        t_fundamental_by_instru_dir=fundamental_by_instru_dir
    )
