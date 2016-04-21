#!/usr/bin/python3
# -*- coding: utf-8 -*-
from io import StringIO
import sys
import sqlalchemy_mssql as ms
import sqlalchemy_pgsql as pg

# if we got one parameter assume it's source tablename
if len(sys.argv) > 1:
    mssql_table_name = sys.argv[1]
else:
    mssql_table_name = "DocFamily"

#using ascii code 31 (unit separator)
FIELD_DELIMITER = u""+chr(31)
# in Postgresql you can choose to keep mixed case characters
# but be careful you will need to quote the table like this  SELECT * FROM "YourMixedCaseTableName"
pgsql_table_name = mssql_table_name.lower()
print("##### BEGIN SYNC MSSQL {src} with PGSQL {dst}#####".format(src=mssql_table_name,
                                                                  dst=pgsql_table_name))
ms_engine = ms.get_engine()
print("### MSSQL {src} contains {num} rows #####".format(src=mssql_table_name,
                                                         num=ms.get_count(ms_engine, mssql_table_name)))
sql_query = ms.get_select_for_postgresql(ms_engine, mssql_table_name)
print("### ABOUT TO RUN SQL QUERY :", sql_query)
ms_cursor = ms_engine.execute(sql_query)
data = StringIO()
pg_engine = pg.get_engine()
pg_num_rows = 0
if pg.does_table_exist(pg_engine, pgsql_table_name):
    pg_num_rows = pg.get_count(pg_engine, pgsql_table_name)
    if pg_num_rows > 0:
        print("### PGSQL  will TRUNCATE {dst} BEFORE SYNC #####".format(dst=pgsql_table_name))
        pg.action_query(pg_engine, 'TRUNCATE ' + pgsql_table_name + ';')
else:
    sql_create_table = ms.get_postgresql_create_sql(ms_engine,mssql_table_name,pgsql_table_name)
    pg.action_query(pg_engine, sql_create_table)
print("### PGSQL {dst} contains {num} rows BEFORE SYNC #####".format(dst=pgsql_table_name,
                                                                     num=pg_num_rows))
while 1:
    row = ms_cursor.fetchone()
    if not row:
        break
    #row_line = FIELD_DELIMITER.join(row).decode("ISO-8859-1").encode("UTF-8") + "\n"

    row_line = pg.escape_copy_string(FIELD_DELIMITER.join(row))+ "\n"
    data.write(row_line)
# print(data.getvalue())
data.seek(0)
pg.bulk_copy(pg_engine, data, pgsql_table_name, FIELD_DELIMITER)
print("### PGSQL {dst} contains {num} rows AFTER SYNC #####".format(dst=pgsql_table_name,
                                                                    num=pg.get_count(pg_engine, pgsql_table_name)))
