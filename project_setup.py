"""
Created by huxo
Update @ 14:30, 2023/9/25
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
    with open("/Deploy/config3.json", "r", encoding="utf-8") as j:
        global_config = json.load(j)
elif this_platform == "LINUX":
    with open("/home/huxo/Deploy/config3.json", "r", encoding="utf-8") as j:
        global_config = json.load(j)
else:
    print("... this platform is {}.".format(this_platform))
    print("... it is not a recognized platform, please check again.")
    sys.exit()

deploy_dir = global_config["deploy_dir"][this_platform]
project_data_root_dir = os.path.join(deploy_dir, "Data")

# --- calendar
calendar_dir = os.path.join(project_data_root_dir, global_config["calendar"]["dir"])
calendar_path = os.path.join(calendar_dir, global_config["calendar"]["file"])

# --- futures data
futures_dir = os.path.join(project_data_root_dir, global_config["futures"]["dir"])
futures_instru_info_path = os.path.join(futures_dir, global_config["futures"]["instrument_info_file"])
futures_by_date_dir = os.path.join(futures_dir, global_config["futures"]["by_date"]["dir"])
futures_by_instrument_dir = os.path.join(futures_dir, global_config["futures"]["by_instrument"]["dir"])
with open(os.path.join(futures_dir, global_config["futures"]["db_struct_file"]), "r", encoding="utf-8") as j:
    db_structs = json.load(j)
futures_md_wds_db_name = global_config["futures"]["md"]["wds_db"]
futures_md_tsdb_db_name = global_config["futures"]["md"]["tsdb_db"]
futures_cm01_db_name = global_config["futures"]["md"]["cm01_db"]
futures_em01_db_name = global_config["futures"]["md"]["em01_db"]
futures_positions_c_db_name = global_config["futures"]["positions"]["c_db"]
futures_positions_e_db_name = global_config["futures"]["positions"]["e_db"]
futures_fundamental_db_name = global_config["futures"]["fundamental"]["db"]

# --- by instrument
by_instru_md_dir = os.path.join(futures_by_instrument_dir, global_config["futures"]["by_instrument"]["md"]["dir"])
by_instru_fd_dir = os.path.join(futures_by_instrument_dir, global_config["futures"]["by_instrument"]["fd"]["dir"])
major_minor_db_name = global_config["futures"]["by_instrument"]["major_minor_db"]
major_return_db_name = global_config["futures"]["by_instrument"]["major_return_db"]
instrument_volume_db_name = global_config["futures"]["by_instrument"]["instrument_volume_db"]
instrument_member_db_name = global_config["futures"]["by_instrument"]["instrument_member_db"]

if __name__ == "__main__":
    from skyrim.winterhold import check_and_mkdir

    check_and_mkdir(futures_by_instrument_dir)
    check_and_mkdir(by_instru_md_dir)
    check_and_mkdir(by_instru_fd_dir)

    print("... directory system for this project has been established.")
