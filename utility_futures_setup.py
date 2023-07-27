"""
Created by huxo
Initialized @ 17:21, 2023/4/17
=========================================
This project is mainly about:
0.  re-organize futures data from Cross-Section to Time Series
1.  Find and locate major and minor contracts
"""

import os
import sys
import json
import platform

# platform confirmation
this_platform = platform.system().upper()
if this_platform == "WINDOWS":
    with open("/Deploy/config.json", "r") as j:
        global_config = json.load(j)
elif this_platform == "LINUX":
    with open("/home/huxo/Deploy/config.json", "r") as j:
        global_config = json.load(j)
else:
    print("... this platform is {}.".format(this_platform))
    print("... it is not a recognized platform, please check again.")
    sys.exit()

deploy_dir = global_config["deploy_dir"][this_platform]
project_data_root_dir = os.path.join(deploy_dir, "Data")

# --- calendar
calendar_dir = os.path.join(project_data_root_dir, global_config["calendar"]["calendar_save_dir"])
calendar_path = os.path.join(calendar_dir, global_config["calendar"]["calendar_save_file"])

# --- TSDB
custom_ts_db_path = os.path.join(project_data_root_dir, global_config["TSDB"]["local_tsdb_dir"])

# --- futures data
futures_dir = os.path.join(project_data_root_dir, global_config["futures"]["futures_save_dir"])
futures_shared_info_path = os.path.join(futures_dir, global_config["futures"]["futures_shared_info_file"])
futures_instru_info_path = os.path.join(futures_dir, global_config["futures"]["futures_instrument_info_file"])

futures_md_dir = os.path.join(futures_dir, global_config["futures"]["md_dir"])
futures_md_structure_path = os.path.join(futures_md_dir, global_config["futures"]["md_structure_file"])
futures_md_db_name = global_config["futures"]["md_db_name"]

futures_fundamental_dir = os.path.join(futures_dir, global_config["futures"]["fundamental_dir"])
futures_fundamental_structure_path = os.path.join(futures_fundamental_dir, global_config["futures"]["fundamental_structure_file"])
futures_fundamental_db_name = global_config["futures"]["fundamental_db_name"]

futures_by_instrument_dir = os.path.join(futures_dir, global_config["futures"]["by_instrument_dir"])
md_by_instru_dir = os.path.join(futures_by_instrument_dir, global_config["futures"]["md_by_instru_dir"])
fundamental_by_instru_dir = os.path.join(futures_by_instrument_dir, global_config["futures"]["fundamental_by_instru_dir"])
major_minor_db_name = global_config["futures"]["major_minor_db"]
major_return_db_name = global_config["futures"]["major_return_db"]
instrument_volume_db_name = global_config["futures"]["instrument_volume_db"]
instrument_member_db_name = global_config["futures"]["instrument_member_db"]

# --- projects
projects_dir = os.path.join(deploy_dir, global_config["projects"]["projects_save_dir"])

if __name__ == "__main__":
    from skyrim.winterhold import check_and_mkdir

    check_and_mkdir(futures_by_instrument_dir)
    check_and_mkdir(md_by_instru_dir)
    check_and_mkdir(fundamental_by_instru_dir)

    print("... directory system for this project has been established.")
