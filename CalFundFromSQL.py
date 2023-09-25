"""
created @ 2023-07-17
0.  this script will reorganize fundamental data from by-date to by-instrument
2.  Since these data are organized by instrument and maybe missing in source, a
    calendar is provided as header to make sure they are all filled.
3.  no argument run_mode is needed, or you can think it is always OVERWRITE
4.  use sql of WDS as source
"""
import datetime
import os
import pandas as pd
from skyrim.whiterun import CCalendar, SetFontGreen
from skyrim.falkreath import CManagerLibReader


def update_fundamental_by_instrument_from_sql(
        bgn_date: str,
        stp_date: str,
        sql_reader: CManagerLibReader,
        calendar: CCalendar,
        by_instru_fd_dir: str,
):
    """

    :param bgn_date:
    :param stp_date:
    :param sql_reader:
    :param calendar:
    :param by_instru_fd_dir:
    :return:
    """

    fundamental_config = {
        "stock": {"values": ["trade_date", "instrument", "in_stock"], "table_name": "STOCK"},
        "basis": {"values": ["trade_date", "instrument", "basis", "basis_rate"], "table_name": "BASIS"},
    }

    # --- set trade_date header
    header_df = pd.DataFrame({"trade_date": calendar.get_iter_list(bgn_date, stp_date, True)})

    for fundamental_data_type, fundamental_data_type_config in fundamental_config.items():
        sql_reader.set_default(t_default_table_name=fundamental_data_type_config["table_name"])
        fundamental_values = fundamental_data_type_config["values"]

        # --- load data from TSDB
        tsdb_df = sql_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=fundamental_values)

        for instrument, instrument_df in tsdb_df.groupby(by="instrument"):
            new_sorted_instrument_df = pd.merge(
                left=header_df, right=instrument_df.drop(labels="instrument", axis=1),
                how="left"
            )
            instrument_file = "{}.{}.csv.gz".format(instrument, fundamental_data_type.upper())
            instrument_path = os.path.join(by_instru_fd_dir, instrument_file)
            new_sorted_instrument_df.to_csv(instrument_path, float_format="%.8f", index=False)
        print(f"... @ {datetime.datetime.now()} fundamental-{SetFontGreen(fundamental_data_type)} between "
              f"[{SetFontGreen(bgn_date)}, {SetFontGreen(stp_date)}) are updated for all instruments ")
    return 0
