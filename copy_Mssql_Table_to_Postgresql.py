#!/usr/bin/python3
# -*- coding: utf-8 -*-
from io import StringIO
import re
import sys
import sqlalchemy_mssql as ms
import sqlalchemy_pgsql as pg

# if we got one parameter assume it's source tablename
if len(sys.argv) > 1:
    mssql_table_name = sys.argv[1]
else:
    mssql_table_name = "Document"

# using ascii code 31 (unit separator)
FIELD_DELIMITER = u""+chr(31)
# in Postgresql you can choose to keep mixed case characters
# but be careful you will need to quote the table like this  SELECT * FROM "YourMixedCaseTableName"
pgsql_table_name = mssql_table_name.lower()
output_filename = "/tmp/" + pgsql_table_name + ".sql"
print("##### BEGIN SYNC MSSQL {src} with PGSQL {dst}#####".format(src=mssql_table_name,
                                                                  dst=pgsql_table_name))
ms_engine = ms.get_engine()
print("### MSSQL {src} contains {num} rows #####".format(src=mssql_table_name,
                                                         num=ms.get_count(ms_engine, mssql_table_name)))
sql_query = ms.get_select_for_postgresql(ms_engine, mssql_table_name)
print("### ABOUT TO RUN SQL QUERY :", sql_query)
ms_cursor = ms_engine.execute(sql_query)
print("### MGSQL DB SERVER COLLATION : {enc}".format(enc=ms.get_dbserver_collation(ms_engine)))
data = StringIO()
pg_engine = pg.get_engine()
# TODO if postgres dbserver encoding is not utf-8 do the convert to right encoding
print("### PGSQL DB SERVER ENCODING : {enc}".format(enc=pg.get_dbserver_encoding(pg_engine)))
print("### PGSQL DB CLIENT ENCODING : {enc}".format(enc=pg.get_dbclient_encoding(pg_engine)))
pg_num_rows = 0
if pg.does_table_exist(pg_engine, pgsql_table_name):
    pg_num_rows = pg.get_count(pg_engine, pgsql_table_name)
    if pg_num_rows > 0:
        print("### PGSQL  will TRUNCATE {dst} BEFORE SYNC #####".format(dst=pgsql_table_name))
        pg.truncate_table(pg_engine, pgsql_table_name )
else:
    sql_create_table = ms.get_postgresql_create_sql(ms_engine,mssql_table_name,pgsql_table_name)
    pg.action_query(pg_engine, sql_create_table)
pg_num_rows = pg.get_count(pg_engine, pgsql_table_name)
print("### PGSQL {dst} contains {num} rows BEFORE SYNC #####".format(dst=pgsql_table_name,
                                                                     num=pg_num_rows))
output_file = open(output_filename,mode="w",encoding="utf-8")
count = 0
total = 0
limit = 20000
regex = re.compile(r'\\', flags=re.IGNORECASE)
while 1:
    row = ms_cursor.fetchone()
    if not row:
        break
    count += 1
    # row_line = FIELD_DELIMITER.join(row).decode("ISO-8859-1").encode("UTF-8") + "\n"
    temp_row_array = []
    for field in row:
        if field == '\\N':
            temp_row_array.append(field)
        else:
            temp_string = regex.sub(r'\\\\', field)
            temp_string = temp_string.replace('\r', '')
            temp_string = temp_string.replace('\n', '\\n')
            temp_row_array.append(temp_string)

    row_line = FIELD_DELIMITER.join(temp_row_array) + "\n"
    #row_line = FIELD_DELIMITER.join(['\\N' if f == '\\N' else (re.sub(r'\\', r'\\\\', f)) for f in row]) + "\n"
    output_file.write(row_line)
    data.write(row_line)
    if count >= limit:
        total += count
        count = 0
        print("### PGSQL flushing data at {num} rows #####".format(num=total))
        # let's flush content
        data.seek(0)
        if pg.bulk_copy(pg_engine, data, pgsql_table_name, FIELD_DELIMITER):
            data.truncate(0)
            data.seek(0)
            output_file.flush()
        else:
            exit("### PGSQL did have problems trying to import this data")

#print(data.getvalue())
data.seek(0)
pg.bulk_copy(pg_engine, data, pgsql_table_name, FIELD_DELIMITER)
output_file.close()
print("### PGSQL {dst} contains {num} rows AFTER SYNC #####".format(dst=pgsql_table_name,
                                                                    num=pg.get_count(pg_engine, pgsql_table_name)))
copy_cmd = "COPY {table} FROM '{file}' WITH DELIMITER AS e{fs!r} NULL AS '{null}' ".format(
    table=pgsql_table_name,
    file=output_filename,
    fs=FIELD_DELIMITER,
    null="\\N")

# COPY docstorage FROM '/tmp/docstorage.sql' WITH DELIMITER AS e'\x1f' NULL AS '\N';
print("### PGSQL you can reload this data in psql with :")
print(copy_cmd)
