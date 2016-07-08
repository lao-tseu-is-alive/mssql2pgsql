#!/usr/bin/python3
from __future__ import print_function
import sqlalchemy_mssql as ms
import sqlalchemy_pgsql as pg
# testing connection to GOELAND MSSQL DB with pyodbc and SqlAlchemy
# BEGIN Connecting to DATABASE with SQLAlchemy pyodbc
ms_engine = ms.get_engine()
pg_engine = pg.get_engine()
# Listing Tables with SQLALCHEMY
ms_table_list = ms.get_tables_list(ms_engine)
ms_table_dict_lower = {ms.convert_to_snake_case(table): table for table in ms_table_list}
ms_table_list_lower = sorted([ms.convert_to_snake_case(table) for table in ms_table_list])
pg_table_set = set(sorted(pg.get_tables_list(pg_engine)))
missing_tables = [table for table in ms_table_list_lower if table not in pg_table_set]
print("### {num} TABLES IN MSSQL        #####".format(num=len(ms_table_list)))
print("### {num} TABLES COPIED TO PGSQL #####".format(num=len(ms_table_list) - len(missing_tables)))
print("### {num} MISSING TABLES IN PGSQL #####".format(num=len(missing_tables)))
for table in missing_tables:
    print("{t}\t{c}\tMSSQL:\t{mst}\t{msc}".format(t=table,
                                                  c=pg.get_count(pg_engine, table),
                                                  mst=ms_table_dict_lower[table],
                                                  msc=ms.get_count(ms_engine, ms_table_dict_lower[table])))
