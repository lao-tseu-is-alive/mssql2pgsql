#!/usr/bin/env python3
import sys
import os
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, base_dir)
from sqlalchemy_mssql import *
import sys

# if we got one parameter assume it's source tablename
if len(sys.argv) > 1:
    mssql_table_name = sys.argv[1]
else:
    mssql_table_name = "Affaire"
flask_table_name = mssql_table_name.lower()

print("--##### BEGIN Connecting to DATABASE with SQLAlchemy pyodbc #####")
ms_engine = get_engine()
print(get_flask_restful_definition_from_mssql(ms_engine, mssql_table_name, flask_table_name))
