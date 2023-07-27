import json
import os
import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CTable, CManagerLibReader, CManagerLibWriter


class CDbByInstrumentBase(object):
    def __init__(self, src_db_structure_path: str, src_db_name: str, src_tab_name: str, src_db_dir: str,
                 calendar: CCalendar):
        # --- init lib reader
        self.m_src_db_struct = src_db_structure_path
        self.m_src_db_name = src_db_name
        self.m_src_tab_name = src_tab_name
        self.m_src_db_dir = src_db_dir
        with open(self.m_src_db_struct, "r") as j:
            src_table_struct = json.load(j)[self.m_src_db_name][self.m_src_tab_name]
        self.src_table = CTable(t_table_struct=src_table_struct)

        # --- set calendar reference
        self.calendar: CCalendar = calendar

    def get_src_reader(self) -> CManagerLibReader:
        db_reader = CManagerLibReader(t_db_save_dir=self.m_src_db_dir, t_db_name=self.m_src_db_name + ".db")
        db_reader.set_default(t_default_table_name=self.src_table.m_table_name)
        return db_reader

    def check_continuity(self, instrument_id: str, run_mode: str, bgn_date: str) -> bool:
        pass

    def get_update_data_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        pass

    def close(self):
        pass

    def print_tips(self):
        pass

    def instrument_loop(self, instrument_ids: list[str], run_mode: str, bgn_date: str, stp_date: str):
        pass

    def main_loop(self, instrument_ids: list[str], run_mode: str, bgn_date: str, stp_date: str):
        t0 = dt.datetime.now()
        self.instrument_loop(instrument_ids, run_mode, bgn_date, stp_date)
        self.close()
        t1 = dt.datetime.now()
        self.print_tips()
        print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
        return 0


class CDbByInstrumentCSV(CDbByInstrumentBase):
    def __init__(self, md_by_instru_dir: str, price_types: list[str], proc_num: int,
                 src_db_structure_path: str, src_db_name: str, src_tab_name: str, src_db_dir: str,
                 calendar: CCalendar):
        super().__init__(src_db_structure_path, src_db_name, src_tab_name, src_db_dir, calendar)
        self.m_md_by_instru_dir = md_by_instru_dir
        self.m_price_types = price_types
        self.m_price_file_prototype = "{}.md.{}.csv.gz"
        self.m_proc_num = proc_num

    def check_continuity(self, instrument_id: str, run_mode: str, bgn_date: str) -> bool:
        if run_mode in ["A", "APPEND"]:
            for price_type in self.m_price_types:
                price_file = self.m_price_file_prototype.format(instrument_id, price_type)
                price_path = os.path.join(self.m_md_by_instru_dir, price_file)
                price_df = pd.read_csv(price_path, dtype={"trade_date": str})
                last_date = price_df["trade_date"].iloc[-1]
                expected_bgn_date = self.calendar.get_next_date(last_date, 1)
                if expected_bgn_date != bgn_date:
                    print(f"... Waring! Last date in {instrument_id:>6s}-{price_type} is {last_date}, and expected bgn_date should be {expected_bgn_date}, which is not equal to bgn_date {bgn_date}.")
                    print(f"... Table for {instrument_id:>6s}-{price_type} is not updated")
                    return False
        return True

    def instrument_loop(self, instrument_ids: list[str], run_mode: str, bgn_date: str, stp_date: str):
        pool = mp.Pool(processes=self.m_proc_num)
        for instrument_id in instrument_ids:
            pool.apply_async(self.get_update_data_by_instrument, args=(instrument_id, run_mode, bgn_date, stp_date))
        pool.close()
        pool.join()
        return 0


class CDbByInstrumentSQL(CDbByInstrumentBase):
    def __init__(self, db_save_dir: str, db_save_name: str, tables: list[CTable], run_mode: str,
                 src_db_structure_path: str, src_db_name: str, src_tab_name: str, src_db_dir: str,
                 calendar: CCalendar, verbose: bool):

        super().__init__(src_db_structure_path, src_db_name, src_tab_name, src_db_dir, calendar)

        # --- init lib writer
        self.m_by_instru_db = CManagerLibWriter(db_save_dir, db_save_name)
        self.m_by_instru_db.initialize_tables(
            t_tables=tables,
            t_remove_existence=run_mode in ["O", "OVERWRITE"],
            t_default_table_name="",
            t_verbose=verbose,
        )

    def check_continuity(self, instrument_id: str, run_mode: str, bgn_date: str) -> bool:
        if run_mode in ["A", "APPEND"]:
            dates_df = self.m_by_instru_db.read(
                t_value_columns=["trade_date"], t_using_default_table=False, t_table_name=instrument_id.replace(".", "_"))
            if len(dates_df) > 0:
                last_date = dates_df["trade_date"].iloc[-1]
                expected_bgn_date = self.calendar.get_next_date(last_date, 1)
            else:
                last_date = expected_bgn_date = "not available"
            if expected_bgn_date != bgn_date:
                print(f"... Waring! Last date in {instrument_id:>6s} is {last_date}, and expected bgn_date should be {expected_bgn_date}, which is not equal to bgn_date {bgn_date}.")
                print(f"... Table for {instrument_id:>6s} in {self.m_by_instru_db.m_db_name} is not updated")
                return False
        return True

    def save(self, update_df: pd.DataFrame, using_index: bool, table_name: str):
        self.m_by_instru_db.update(
            t_update_df=update_df,
            t_using_index=using_index,
            t_using_default_table=False,
            t_table_name=table_name,
        )
        return 0

    def instrument_loop(self, instrument_ids: list[str], run_mode: str, bgn_date: str, stp_date: str):
        for instrument_id in instrument_ids:
            self.get_update_data_by_instrument(instrument_id, run_mode, bgn_date, stp_date)
        return 0

    def close(self):
        self.m_by_instru_db.close()
        return 0
