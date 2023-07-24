import json
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CTable, CManagerLibReader, CManagerLibWriter


class CDbByInstrument(object):
    def __init__(self, db_save_dir: str, db_save_name: str, tables: list[CTable], run_mode: str,
                 src_tab_name: str, futures_md_structure_path: str, futures_md_db_name: str, futures_md_dir: str,
                 calendar: CCalendar,
                 verbose: bool = False):
        # --- init lib writer
        self.m_by_instru_db = CManagerLibWriter(db_save_dir, db_save_name)
        self.m_by_instru_db.initialize_tables(
            t_tables=tables,
            t_remove_existence=run_mode in ["O", "OVERWRITE"],
            t_default_table_name="",
            t_verbose=verbose,
        )

        # --- init lib reader
        self.m_src_tab_name = src_tab_name
        self.m_ftr_md_struct = futures_md_structure_path
        self.m_ftr_md_db_name = futures_md_db_name
        self.m_ftr_md_dir = futures_md_dir
        with open(self.m_ftr_md_struct, "r") as j:
            md_table_struct = json.load(j)[self.m_ftr_md_db_name][self.m_src_tab_name]
        md_table = CTable(t_table_struct=md_table_struct)
        self.md_db = CManagerLibReader(t_db_save_dir=self.m_ftr_md_dir, t_db_name=self.m_ftr_md_db_name + ".db")
        self.md_db.set_default(t_default_table_name=md_table.m_table_name)

        self.calendar: CCalendar = calendar

    def get_update_df_by_instrument(self, instrument_id: str, run_mode: str, bgn_date: str, stp_date: str):
        pass

    def save(self, table_name: str, update_df: pd.DataFrame):
        self.m_by_instru_db.update(
            t_update_df=update_df,
            t_using_index=False,
            t_table_name=table_name,
            t_using_default_table=False,
        )
        return 0

    def close(self):
        self.md_db.close()
        self.m_by_instru_db.close()
        return 0
