#!/usr/bin/python3
import sys
import os
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, base_dir)
import sqlalchemy_mssql as ms
# testing connection to GOELAND MSSQL DB with pyodbc and SqlAlchemy
# BEGIN Connecting to DATABASE with SQLAlchemy pyodbc
engine = ms.get_engine()
# Listing Tables with SQLALCHEMY
table_list = ms.get_tables_list(engine)
#print(type(table_list))
for table in table_list:
    print(table)
