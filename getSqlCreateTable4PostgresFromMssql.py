#!/usr/bin/env python
from __future__ import print_function
from sqlalchemy_mssql import *
import sys

# if we got one parameter assume it's source tablename
if len(sys.argv) > 1:
    mssql_table_name = sys.argv[1]
else:
    mssql_table_name = "Document"
# in Postgresql you can choose to keep mixed case characters
# but be careful you will need to quote the table in all your queries
# like this  SELECT * FROM "YourMixedCaseTableName"
pgsql_table_name = mssql_table_name.lower()

print("--##### BEGIN Connecting to DATABASE with SQLAlchemy pyodbc #####")
ms_engine = get_engine()
print(get_postgresql_create_sql(ms_engine, mssql_table_name, pgsql_table_name))
