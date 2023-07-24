"""
created @ 2023-04-17
0.  this script is based on the results of cal_0_major_minor
1.  the major contract of T-day is decided by the information of T-1 day, so the major return of T
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

from utility_futures_setup import sys, os, json, dt, np, pd
from skyrim.falkreath import CManagerLibReader, CTable


def cal_major_return(x: pd.Series, t_this_prc_lbl: str, t_prev_prc_lbl: str, t_rtn_scale: float) -> float:
    res = x[t_this_prc_lbl] / x[t_prev_prc_lbl] - 1
    return 0 if np.isnan(res) else np.round(res * t_rtn_scale, 8)


def update_major_return(
        t_instrument_id: str,
        t_run_mode: str, t_bgn_date: str, t_stp_date: str,
        t_src_tab_name: str,
        t_futures_md_structure_path: str, t_futures_md_db_name: str, t_futures_md_dir: str,
        t_major_minor_dir: str, t_major_return_dir: str, t_mkt_idx_dir: str,
        trailing_window: int = 60, return_scale: int = 100,
        price_type: str = "close", vo_adj_split_date: str = "20200101",
):
    """

    :param t_instrument_id:
    :param t_run_mode: O, OVERWRITE; A, APPEND.
    :param t_bgn_date:
    :param t_stp_date:
    :param t_src_tab_name:
    :param t_futures_md_structure_path:
    :param t_futures_md_db_name:
    :param t_futures_md_dir:
    :param t_major_minor_dir:
    :param t_major_return_dir:
    :param t_mkt_idx_dir:
    :param trailing_window:
    :param return_scale:
    :param price_type:
    :param vo_adj_split_date:
    :return:
    """
    instrument, exchange = t_instrument_id.split(".")
    this_prc_lbl = price_type
    prev_prc_lbl = "prev_{}".format(price_type)

    # --- load major table
    input_file = "major_minor.{}.csv.gz".format(t_instrument_id)
    input_path = os.path.join(t_major_minor_dir, input_file)
    input_df = pd.read_csv(input_path, dtype={"trade_date": str})
    input_df["prev_trade_date"] = input_df["trade_date"].shift(1)
    filter_time_window = (input_df["trade_date"] >= t_bgn_date) & (input_df["trade_date"] < t_stp_date)
    input_df = input_df.loc[filter_time_window]

    # --- set volume adjustment ratio
    if exchange in ["CFE"]:
        input_df["vo_adj_ratio"] = 1
    else:
        input_df["vo_adj_ratio"] = [2 if trade_date < vo_adj_split_date else 1 for trade_date in input_df["trade_date"]]

    # --- init lib reader
    with open(t_futures_md_structure_path, "r") as j:
        md_table_struct = json.load(j)[t_futures_md_db_name][t_src_tab_name]
    md_table = CTable(t_table_struct=md_table_struct)
    md_db = CManagerLibReader(t_db_save_dir=t_futures_md_dir, t_db_name=t_futures_md_db_name + ".db")
    md_db.set_default(t_default_table_name=md_table.m_table_name)

    # --- load historical data
    md_df = md_db.read_by_instrument_and_time_window(
        t_instrument=instrument,
        t_value_columns=["trade_date", "loc_id", "open", "high", "low", "close", "volume", "amount", "oi"],
        t_bgn_date=(dt.datetime.strptime(t_bgn_date, "%Y%m%d") - dt.timedelta(days=trailing_window)).strftime("%Y%m%d"),
        t_stp_date=t_stp_date,
    ).rename(mapper={"loc_id": "contract"}, axis=1)
    md_db.close()

    # --- fillna
    md_df[["open", "high", "low", "close"]] = md_df[["open", "high", "low", "close"]].fillna(np.nan)
    md_df[["volume", "amount", "oi"]] = md_df[["volume", "amount", "oi"]].fillna(0)

    # --- update price
    input_df = pd.merge(
        left=input_df, right=md_df,
        left_on=["trade_date", "n_contract"],
        right_on=["trade_date", "contract"],
        how="left"
    )

    input_df = pd.merge(
        left=input_df, right=md_df[["trade_date", "contract", price_type]],
        left_on=["prev_trade_date", "n_contract"],
        right_on=["trade_date", "contract"],
        how="left", suffixes=("", "_prev")
    ).rename(mapper={price_type + "_prev": prev_prc_lbl}, axis=1)

    # --- adjust volume amount openInterest
    input_df[["volume", "amount", "oi"]] = input_df[["volume", "amount", "oi"]].div(
        input_df["vo_adj_ratio"], axis="index").fillna(0)

    # --- major return
    input_df["major_return"] = input_df[[this_prc_lbl, prev_prc_lbl]].apply(
        cal_major_return, t_this_prc_lbl=this_prc_lbl, t_prev_prc_lbl=prev_prc_lbl, t_rtn_scale=return_scale,
        axis=1
    )

    # --- column selection
    major_return_df = input_df[[
        "trade_date", "n_contract",
        "open", "high", "low", "close", prev_prc_lbl, "volume", "amount", "oi",
        "major_return"]].copy()

    # --- set destination
    major_return_file = "major_return.{}.{}.csv.gz".format(t_instrument_id, price_type)
    major_return_path = os.path.join(t_major_return_dir, major_return_file)
    if t_run_mode.upper() in ["A", "APPEND"]:
        old_major_return_df = pd.read_csv(major_return_path, dtype={"trade_date": str}).drop(labels=["instru_idx"], axis=1)
        new_major_return_df = pd.concat([old_major_return_df, major_return_df])
        new_major_return_df = new_major_return_df.drop_duplicates(keep="first").sort_values("trade_date", ascending=True)
        if len(new_major_return_df) - len(old_major_return_df) != 1:
            print("-" * 60)
            print("Warning! Size of increment data != 1")
            print("size before update:{}".format(len(old_major_return_df)))
            print("size after  update:{}".format(len(new_major_return_df)))
            sys.exit()

    else:
        new_major_return_df = major_return_df

    # --- set instrument index
    new_major_return_df["instru_idx"] = (new_major_return_df["major_return"] / return_scale + 1).cumprod()

    # --- save
    new_major_return_df.to_csv(major_return_path, float_format="%.8f", index=False)

    # --- save instrument index
    custom_instru_idx_df = new_major_return_df[["trade_date", "instru_idx"]]
    custom_instru_idx_file = "{}.index.csv.gz".format(t_instrument_id)
    custom_instru_idx_path = os.path.join(t_mkt_idx_dir, custom_instru_idx_file)
    custom_instru_idx_df.to_csv(custom_instru_idx_path, float_format="%.8f", index=False)

    # print("| {} | {:>6s} | {:>8s} | Major return calculated |".format(dt.datetime.now(), t_instrument_id, price_type))
    return 0


if __name__ == "__main__":
    from utility_futures_setup import futures_md_structure_path, futures_md_db_name, futures_md_dir
    from utility_futures_setup import major_minor_dir, major_return_dir, instru_idx_dir

    test_instrument = "CU.SHF"
    # test_instrument = "ZC.CZC"

    update_major_return(
        t_instrument_id=test_instrument,
        t_futures_md_structure_path=futures_md_structure_path,
        t_futures_md_db_name=futures_md_db_name,
        t_futures_md_dir=futures_md_dir,
        t_major_minor_dir=major_minor_dir,
        t_major_return_dir=major_return_dir,
        t_mkt_idx_dir=instru_idx_dir,
        t_run_mode="O",
        t_bgn_date="20120101", t_stp_date="20230417",
        t_src_tab_name="CTable",
    )

    update_major_return(
        t_instrument_id=test_instrument,
        t_futures_md_structure_path=futures_md_structure_path,
        t_futures_md_db_name=futures_md_db_name,
        t_futures_md_dir=futures_md_dir,
        t_major_minor_dir=major_minor_dir,
        t_major_return_dir=major_return_dir,
        t_mkt_idx_dir=instru_idx_dir,
        t_run_mode="A",
        t_bgn_date="20230417", t_stp_date="20230418",
        t_src_tab_name="CTable",
    )
