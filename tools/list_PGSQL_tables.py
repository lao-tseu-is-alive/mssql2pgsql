#!/usr/bin/python3
import sys
import os
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, base_dir)
import sqlalchemy_pgsql as pg
# testing connection to GOELAND MSSQL DB with pyodbc and SqlAlchemy
# BEGIN Connecting to DATABASE with SQLAlchemy pyodbc
engine = pg.get_engine()
# Listing Tables with SQLALCHEMY
table_list = pg.get_tables_list(engine)
for table in table_list:
#    print("{tbl}".format(tbl=table))
     print("DROP TABLE {tbl};".format(tbl=table))
