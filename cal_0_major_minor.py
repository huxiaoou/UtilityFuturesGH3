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

from utility_futures_setup import sys, os, json, dt, pd
from skyrim.falkreath import CManagerLibReader, CTable


def parse_n_contract_and_d_contract(t_vol_srs: pd.Series, instrument: str, exchange: str):
    _trade_date = t_vol_srs.name
    _trade_month = _trade_date[2:6]  # format = "YYMM"
    _trade_month_contract = instrument + _trade_month + "." + exchange
    # n_contract(or major contract) will always behind this trade month
    if instrument.upper() in ["IC", "IF", "IH", "IM"]:
        n_contract = t_vol_srs[t_vol_srs.index >= _trade_month_contract].idxmax()
    else:
        n_contract = t_vol_srs[t_vol_srs.index > _trade_month_contract].idxmax()
    d_contract = t_vol_srs[t_vol_srs.index > n_contract].idxmax()
    return n_contract, d_contract


def update_major_minor(
        t_instrument_id: str,
        t_run_mode: str, t_bgn_date: str, t_stp_date: str,
        t_src_tab_name: str,
        t_volume_moving_aver_n: int, t_volume_shift_n: int,
        t_futures_md_structure_path: str, t_futures_md_db_name: str, t_futures_md_dir: str,
        t_major_minor_dir: str,
        trailing_window: int = 60
):
    """

    :param t_instrument_id: "CU.SHF"
    :param t_run_mode: O, OVERWRITE; A, APPEND.
    :param t_bgn_date: included,
    :param t_stp_date: not included if in "O" mode; should not be provided in "A" mode
    :param t_src_tab_name:
    :param t_volume_moving_aver_n:
    :param t_volume_shift_n:
    :param t_futures_md_structure_path:
    :param t_futures_md_db_name:
    :param t_futures_md_dir:
    :param t_major_minor_dir:
    :param trailing_window: Make sure trailing is least 60 days, or there would be a bug. If trailing window is too short,
                            and volume is 0, n_contract chosen by this function in "O" mode maybe different from the one
                            in "A" mode.
    :return:
    """
    instrument, exchange = t_instrument_id.split(".")

    # --- init lib reader
    with open(t_futures_md_structure_path, "r") as j:
        md_table_struct = json.load(j)[t_futures_md_db_name][t_src_tab_name]
    md_table = CTable(t_table_struct=md_table_struct)
    md_db = CManagerLibReader(t_db_save_dir=t_futures_md_dir, t_db_name=t_futures_md_db_name + ".db")
    md_db.set_default(t_default_table_name=md_table.m_table_name)

    # --- load historical data
    md_df = md_db.read_by_instrument_and_time_window(
        t_instrument=instrument,
        t_value_columns=["trade_date", "loc_id", "volume"],
        t_bgn_date=(dt.datetime.strptime(t_bgn_date, "%Y%m%d") - dt.timedelta(days=trailing_window)).strftime(
            "%Y%m%d"),
        t_stp_date=t_stp_date,
    ).rename(mapper={"loc_id": "contract"}, axis=1)
    md_db.close()

    # --- pivot
    pivot_volume_df = pd.pivot_table(data=md_df, values="volume", index="trade_date", columns="contract").fillna(0)
    pivot_volume_df_sorted = pivot_volume_df.sort_index(axis=1).sort_index(axis=0)
    volume_mov_aver_df = pivot_volume_df_sorted.rolling(window=t_volume_moving_aver_n).mean()
    volume_mov_aver_df = volume_mov_aver_df.shift(t_volume_shift_n)
    if t_run_mode in ["O", "OVERWRITE"]:
        volume_mov_aver_df = volume_mov_aver_df.fillna(method="bfill")  # for first 1 rows
    else:
        # drop first few rows, where the moving average volume is not correct
        volume_mov_aver_df = volume_mov_aver_df.tail(len(volume_mov_aver_df) - t_volume_shift_n - t_volume_moving_aver_n)

    # main loop to get major and minor contracts
    volume_mov_aver_df["n_contract"], volume_mov_aver_df["d_contract"] = \
        zip(*volume_mov_aver_df.apply(parse_n_contract_and_d_contract, args=(instrument, exchange), axis=1))
    major_minor_df = volume_mov_aver_df[["n_contract", "d_contract"]].reset_index()
    filter_dates = (major_minor_df["trade_date"] >= t_bgn_date) & (major_minor_df["trade_date"] < t_stp_date)
    major_minor_df = major_minor_df.loc[filter_dates]

    # save according to run_mode
    major_minor_file = "major_minor.{}.csv.gz".format(t_instrument_id)
    major_minor_path = os.path.join(t_major_minor_dir, major_minor_file)
    if t_run_mode.upper() in ["O", "OVERWRITE"]:
        new_major_minor_df = major_minor_df
    else:
        old_major_minor_df = pd.read_csv(major_minor_path, dtype=str)
        new_major_minor_df = pd.concat([old_major_minor_df, major_minor_df])
        new_major_minor_df = new_major_minor_df.drop_duplicates(keep="first").sort_values("trade_date", ascending=True)
        if len(new_major_minor_df) - len(old_major_minor_df) != 1:
            print("-" * 60)
            print("Warning! Size of increment data != 1")
            print("-" * 60)
            print("existing  file:")
            print(old_major_minor_df.tail(6))
            print("-" * 60)
            print("increment file:")
            print(major_minor_df.tail(6))
            print("-" * 60)
            print("new       file:")
            print(new_major_minor_df.tail(6))
            print("-" * 60)
            print("size before update:{:>4d}".format(len(old_major_minor_df)))
            print("size of  increment:{:>4d}".format(len(major_minor_df)))
            print("size after  update:{:>4d}".format(len(new_major_minor_df)))
            print("-" * 60)
            sys.exit()

    new_major_minor_df.to_csv(major_minor_path, index=False)
    print("| {} | {:>6s} | {} | {} | Major and Minor contracts calculated |".format(
        dt.datetime.now(), t_instrument_id, t_bgn_date, t_stp_date))
    return 0


if __name__ == "__main__":
    from utility_futures_setup import futures_md_structure_path, futures_md_db_name, futures_md_dir
    from utility_futures_setup import major_minor_dir

    test_instrument = "IF.CFE"
    # test_instrument = "CU.SHF"

    instru, _ = test_instrument.split(".")
    if instru.upper() in ["IC", "IF", "IH", "IM"]:
        vol_mov_ave, vol_shift = 1, 0
    else:
        vol_mov_ave, vol_shift = 3, 1

    update_major_minor(
        t_instrument_id=test_instrument,
        t_bgn_date="20120101", t_stp_date="20230425",
        t_src_tab_name="CTable",
        t_volume_moving_aver_n=vol_mov_ave, t_volume_shift_n=vol_shift,
        t_futures_md_structure_path=futures_md_structure_path,
        t_futures_md_db_name=futures_md_db_name,
        t_futures_md_dir=futures_md_dir,
        t_major_minor_dir=major_minor_dir,
        t_run_mode="O"
    )

    update_major_minor(
        t_instrument_id=test_instrument,
        t_bgn_date="20230425", t_stp_date="20230426",
        t_src_tab_name="CTable",
        t_volume_moving_aver_n=vol_mov_ave, t_volume_shift_n=vol_shift,
        t_futures_md_structure_path=futures_md_structure_path,
        t_futures_md_db_name=futures_md_db_name,
        t_futures_md_dir=futures_md_dir,
        t_major_minor_dir=major_minor_dir,
        t_run_mode="A"
    )
