import os
import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.whiterun import CCalendar, SetFontRed, SetFontYellow, SetFontBlue
from skyrim.falkreath import CTable, CManagerLibReader, CManagerLibWriter


class CDbByInstrumentBase(object):
    def __init__(self, proc_num: int, src_db_name: str, src_tab_name: str, src_db_dir: str,
                 src_db_structs: dict[str, dict],
                 calendar: CCalendar):
        # --- init lib reader
        self.m_src_db_dir = src_db_dir
        self.m_src_db_name = src_db_name
        self.m_src_tab_name = src_tab_name
        src_table_struct = src_db_structs[src_db_name][src_tab_name]
        self.src_table = CTable(t_table_struct=src_table_struct)

        # --- set calendar reference
        self.calendar: CCalendar = calendar

        self.m_proc_num = proc_num

    def _get_src_reader(self) -> CManagerLibReader:
        db_reader = CManagerLibReader(t_db_save_dir=self.m_src_db_dir, t_db_name=self.m_src_db_name)
        db_reader.set_default(t_default_table_name=self.src_table.m_table_name)
        return db_reader

    def _check_continuity(self, instrument_id: str, run_mode: str, bgn_date: str) -> int:
        pass

    def _get_update_data_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str) -> tuple[str, list[tuple[str, pd.DataFrame]]] | None:
        pass

    def _save(self, update_dfs: list[tuple[str, list[tuple[str, pd.DataFrame]]]], run_mode: str):
        pass

    def _instrument_loop(self, instrument_ids: list[str], run_mode: str, bgn_date: str, stp_date: str):
        update_data = []
        pool = mp.Pool(processes=self.m_proc_num)
        for instrument_id in instrument_ids:
            res = pool.apply_async(self._get_update_data_by_instrument, args=(instrument_id, run_mode, bgn_date, stp_date))
            update_data.append(res)
        pool.close()
        pool.join()
        update_dfs: list[tuple[str, list[tuple[str, pd.DataFrame]]]] = [_.get() for _ in update_data]
        self._save(update_dfs, run_mode)
        return 0

    def _print_tips(self):
        pass

    def main_loop(self, instrument_ids: list[str], run_mode: str, bgn_date: str, stp_date: str):
        t0 = dt.datetime.now()
        self._instrument_loop(instrument_ids, run_mode, bgn_date, stp_date)
        t1 = dt.datetime.now()
        self._print_tips()
        print(f"... total time consuming: {SetFontBlue(f'{(t1 - t0).total_seconds():.2f}')} seconds")
        return 0


class CDbByInstrumentCSV(CDbByInstrumentBase):
    def __init__(self, by_instru_md_dir: str, price_types: list[str], **kwargs):
        super().__init__(**kwargs)
        self.m_by_instru_md_dir = by_instru_md_dir
        self.m_price_types = price_types
        self.m_price_file_prototype = "{}.md.{}.csv.gz"

    def _check_continuity(self, instrument_id: str, run_mode: str, bgn_date: str) -> int:
        if run_mode in ["A", "APPEND"]:
            for price_type in self.m_price_types:
                price_file = self.m_price_file_prototype.format(instrument_id, price_type)
                price_path = os.path.join(self.m_by_instru_md_dir, price_file)
                price_df = pd.read_csv(price_path, dtype={"trade_date": str})
                last_date = price_df["trade_date"].iloc[-1]
                expected_bgn_date = self.calendar.get_next_date(last_date, 1)
                if expected_bgn_date == bgn_date:
                    return 0
                elif expected_bgn_date < bgn_date:
                    print(f"... Waring! Last date of  {SetFontRed(f'{instrument_id:>6s}-{price_type}')} is {last_date}, "
                          f"and expected bgn_date should be {SetFontRed(expected_bgn_date)}, but input bgn_date = {bgn_date}, "
                          f"some days may be {SetFontYellow('omitted')}")
                    return 1
                else:  # expected_bgn_date > bgn_date:
                    print(f"... Waring! Last date of  {SetFontRed(f'{instrument_id:>6s}-{price_type}')} is {last_date}, "
                          f"and expected bgn_date should be {SetFontRed(expected_bgn_date)}, but input bgn_date = {bgn_date}, "
                          f"some days may be {SetFontRed('overwritten')}")
                    return 2
        return 0

    def _save(self, update_dfs: list[tuple[str, list[tuple[str, pd.DataFrame]]]], run_mode: str):
        for res in update_dfs:  # instrument loop
            if res:
                instrument_id, dfs_list = res
                for price_type, new_price_df in dfs_list:
                    price_file = self.m_price_file_prototype.format(instrument_id, price_type)
                    price_path = os.path.join(self.m_by_instru_md_dir, price_file)
                    new_price_df.to_csv(price_path, float_format="%.3f", index=False)
        return 0


class CDbByInstrumentSQL(CDbByInstrumentBase):
    def __init__(self, db_save_dir: str, db_save_name: str, tables: list[CTable], run_mode: str, verbose: bool, **kwargs):

        super().__init__(**kwargs)
        self.m_dst_db_save_dir, self.m_dst_db_save_name = db_save_dir, db_save_name
        self.m_manager_tables = {_.m_table_name: _ for _ in tables}

        # --- init lib writer
        m_by_instru_db = CManagerLibWriter(self.m_dst_db_save_dir, self.m_dst_db_save_name)
        m_by_instru_db.initialize_tables(
            t_tables=tables,
            t_remove_existence=run_mode in ["O", "OVERWRITE"],
            t_default_table_name="",
            t_verbose=verbose,
        )
        m_by_instru_db.close()

    def _check_continuity(self, instrument_id: str, run_mode: str, bgn_date: str) -> int:
        """

        :param instrument_id:
        :param run_mode:
        :param bgn_date:
        :return: 0: No error
                 1: Not continued warning, gap exists
                 2: Overlapping warning, some data maybe overwrite
        """
        if run_mode in ["A", "APPEND"]:
            m_by_instru_db = CManagerLibReader(self.m_dst_db_save_dir, self.m_dst_db_save_name)
            m_by_instru_db.set_default(t_default_table_name=instrument_id.replace(".", "_"))
            dates_df = m_by_instru_db.read(t_value_columns=["trade_date"])
            m_by_instru_db.close()
            if len(dates_df) > 0:
                last_date = dates_df["trade_date"].iloc[-1]
                expected_bgn_date = self.calendar.get_next_date(last_date, 1)
            else:
                last_date = "not available"
                expected_bgn_date = "20120104"
            if expected_bgn_date == bgn_date:
                return 0
            elif expected_bgn_date < bgn_date:
                print(f"... Waring! Last date of {SetFontRed(f'{instrument_id:>6s}')} is {last_date}, "
                      f"and expected bgn_date should be {SetFontRed(expected_bgn_date)}, but input bgn_date = {bgn_date}, "
                      f"some days may be {SetFontYellow('omitted')}")
                return 1
            else:  # expected_bgn_date > bgn_date
                print(f"... Waring! Last date of {SetFontRed(f'{instrument_id:>6s}')} is {last_date}, "
                      f"and expected bgn_date should be {SetFontRed(expected_bgn_date)}, but input bgn_date = {bgn_date}, "
                      f"some days may be {SetFontRed('overwritten')}")
                return 2
        return 0

    def _save(self, update_dfs: list[tuple[str, list[tuple[str, pd.DataFrame]]]], run_mode: str):
        for res in update_dfs:  # instrument loop
            if res:
                instrument_id, dfs_list = res
                _, update_df = dfs_list[0]
                if not update_df.empty:
                    using_index = False
                    table_name = instrument_id.replace(".", "_")
                    m_by_instru_db = CManagerLibWriter(self.m_dst_db_save_dir, self.m_dst_db_save_name)
                    m_by_instru_db.initialize_table(t_table=self.m_manager_tables[table_name], t_remove_existence=run_mode in ["O"])
                    m_by_instru_db.update(
                        t_update_df=update_df,
                        t_using_index=using_index,
                        t_using_default_table=False,
                        t_table_name=table_name,
                    )
                    m_by_instru_db.close()
        return 0
