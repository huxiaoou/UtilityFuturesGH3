# Description

This project is designed to transpose futures data from by-date to by-instru.

Main differences from previous versions:

+ User can use an argument tsdb/sql to choose the data source.
+ Only major_minor and major_return are kept, and md part is abandoned.
+ An instru-based K-Line data is appended.
+ sqlite database is introduced to replace the csv-files.
+ VOLUME_SHIFT_N are all set to 0.
+ run_mode, bgn_date, stp_date are redesigned.
+ project structure are refactored to use main.py as entrance ONLY. 
