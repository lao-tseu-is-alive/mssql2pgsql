#!/usr/bin/python3
import sys
import os
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, base_dir)
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
    print(ms_table_dict_lower[table])
    # print("{t}\t{c}\tMSSQL:\t{mst}\t{msc}".format(t=table,
    #                                               c=pg.get_count(pg_engine, table),
    #                                               mst=ms_table_dict_lower[table],
    #                                               msc=ms.get_count(ms_engine, ms_table_dict_lower[table])))

print("### TABLES WITH MORE THEN 5% MISSING RECORDS IN PGSQL #####")
for table in sorted(pg.get_tables_list(pg_engine)):
    if table in ms_table_dict_lower:
        num_rows_pgsql = pg.get_count(pg_engine, table)
        num_rows_mssql = ms.get_count(ms_engine, ms_table_dict_lower[table])
        # we allow 5% difference
        tolerance = num_rows_mssql * 0.05
        if (num_rows_mssql - num_rows_pgsql) > tolerance:
            print("{t}\t{c}\tMSSQL:\t{mst}\t{msc}".format(t=table,
                                                          c=pg.get_count(pg_engine, table),
                                                          mst=ms_table_dict_lower[table],
                                                          msc=ms.get_count(ms_engine, ms_table_dict_lower[table])))
