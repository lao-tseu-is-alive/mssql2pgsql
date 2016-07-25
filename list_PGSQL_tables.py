#!/usr/bin/python3
from __future__ import print_function
import sqlalchemy_pgsql as pg
# testing connection to GOELAND MSSQL DB with pyodbc and SqlAlchemy
# BEGIN Connecting to DATABASE with SQLAlchemy pyodbc
engine = pg.get_engine()
# Listing Tables with SQLALCHEMY
table_list = pg.get_tables_list(engine)
for table in table_list:
#    print("{tbl}".format(tbl=table))
     print("DROP TABLE {tbl};".format(tbl=table))
