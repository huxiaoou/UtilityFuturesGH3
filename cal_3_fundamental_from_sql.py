"""
created @ 2023-07-17
0.  this script will reorganize fundamental data from by-date to by-instrument
2.  Since these data are organized by instrument and maybe missing in source, a
    calendar is provided as header to make sure they are all filled.
3.  no argument run_mode is needed, or you can think it is always OVERWRITE
4.  use sql of WDS as source
"""

import os
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CManagerLibReader


def update_fundamental_by_instrument_from_sql(
        t_bgn_date: str,
        t_stp_date: str,
        t_sql_reader: CManagerLibReader,
        t_calendar: CCalendar,
        t_fundamental_by_instru_dir: str,
):
    """

    :param t_bgn_date:
    :param t_stp_date:
    :param t_sql_reader:
    :param t_calendar:
    :param t_fundamental_by_instru_dir:
    :return:
    """

    fundamental_config = {
        "stock": {"values": ["trade_date", "instrument", "in_stock"], "table_name": "Stock"},
        "basis": {"values": ["trade_date", "instrument", "basis", "basis_rate"], "table_name": "BasisW"},
    }

    # --- set trade_date header
    header_df = pd.DataFrame({"trade_date": t_calendar.get_iter_list(t_bgn_date, t_stp_date, True)})

    for fundamental_data_type, fundamental_data_type_config in fundamental_config.items():
        t_sql_reader.set_default(t_default_table_name=fundamental_data_type_config["table_name"])
        fundamental_values = fundamental_data_type_config["values"]

        # --- load data from TSDB
        tsdb_df = t_sql_reader.read_by_conditions(t_conditions=[
            ("trade_date", ">=", t_bgn_date),
            ("trade_date", "<", t_stp_date),
        ], t_value_columns=fundamental_values)

        for instrument, instrument_df in tsdb_df.groupby(by="instrument"):
            new_sorted_instrument_df = pd.merge(
                left=header_df, right=instrument_df.drop(labels="instrument", axis=1),
                how="left"
            )
            instrument_file = "{}.{}.csv.gz".format(instrument, fundamental_data_type.upper())
            instrument_path = os.path.join(t_fundamental_by_instru_dir, instrument_file)
            new_sorted_instrument_df.to_csv(instrument_path, float_format="%.8f", index=False)
            # print("| {} | {:>8s} | {:>8s} | fundamental |".format(dt.datetime.now(), wind_code, fundamental_data_type))
    return 0


if __name__ == "__main__":
    from utility_futures_setup import calendar_path, futures_fundamental_dir, futures_fundamental_db_name
    from utility_futures_setup import fundamental_by_instru_dir

    sql_reader = CManagerLibReader(t_db_save_dir=futures_fundamental_dir, t_db_name=futures_fundamental_db_name + ".db")
    calendar = CCalendar(calendar_path)
    update_fundamental_by_instrument_from_sql(
        t_bgn_date="20120101",
        t_stp_date="20230717",
        t_sql_reader=sql_reader,
        t_calendar=calendar,
        t_fundamental_by_instru_dir=fundamental_by_instru_dir
    )
