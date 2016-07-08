#!/usr/bin/python3
from __future__ import print_function
import sqlalchemy_mssql as ms
# testing connection to GOELAND MSSQL DB with pyodbc and SqlAlchemy
# BEGIN Connecting to DATABASE with SQLAlchemy pyodbc
engine = ms.get_engine()
# Listing Tables with SQLALCHEMY
table_list = ms.get_tables_list(engine)
print(type(table_list))
for table in table_list:
    print(table)
