#!/usr/bin/python3
# -*- coding: utf-8 -*-
from io import StringIO
import re
import sys
import sqlalchemy_mssql as ms
from sqlalchemy.sql import text
import sqlalchemy_pgsql as pg

NUM_ROWS_IN_BUFFER = 20000

# with this script you can replicate all tables from mssql database with a bash command like this :
# time for i in `./list_MSSQL_tables.py`;do ((time ./copy_Mssql_Table_to_Postgresql.py $i) >> R20160708.txt  2>&1);done;

# if we got one parameter assume its source table name
if len(sys.argv) > 1:
    mssql_table_name = sys.argv[1]
else:
    mssql_table_name = "Employe"

# using ascii code 31 (unit separator)
FIELD_DELIMITER = u"" + chr(31)
# in Postgresql you can choose to keep mixed case characters
# but be careful you will need to quote the table like this  SELECT * FROM "YourMixedCaseTableName"
pgsql_table_name = ms.convert_to_snake_case(mssql_table_name)
output_filename = "/tmp/" + pgsql_table_name + ".sql"
print("##### BEGIN SYNC MSSQL {src} with PGSQL {dst}#####".format(src=mssql_table_name,
                                                                  dst=pgsql_table_name))
ms_engine = ms.get_engine()
table_list = ms.get_tables_list(ms_engine)
if mssql_table_name not in table_list:
    print("### ERROR : table {t} was not found in mssql database ".format(t=mssql_table_name))
    exit(1)
ms_num_rows_origin = ms.get_count(ms_engine, mssql_table_name)
print("### MSSQL {src} contains {num} rows #####".format(src=mssql_table_name,
                                                         num=ms_num_rows_origin))
sql_query = ms.get_select_for_postgresql(ms_engine, mssql_table_name)
print("### ABOUT TO RUN SQL QUERY :\n", sql_query)
# print("### BEFORE \n")
# ms.query(ms_engine, sql_query)
# print("### NEXT \n")
with ms_engine.connect() as connection:
    ms_cursor = connection.execute(text(sql_query))
    print("### MSSQL DB SERVER COLLATION : {enc}".format(enc=ms.get_dbserver_collation(ms_engine)))
    data = StringIO()
    pg_engine = pg.get_engine()
    print("### PGSQL DB SERVER ENCODING : {enc}".format(enc=pg.get_dbserver_encoding(pg_engine)))
    print("### PGSQL DB CLIENT ENCODING : {enc}".format(enc=pg.get_dbclient_encoding(pg_engine)))
    pg_num_rows = 0
    # TODO check if structure of table has changed in mssql if so drop and recreate in postgresql
    if pg.does_table_exist(pg_engine, pgsql_table_name):
        pg_num_rows = pg.get_count(pg_engine, pgsql_table_name)
        if pg_num_rows > 0:
            print("### PGSQL  will TRUNCATE {dst} BEFORE SYNC #####".format(dst=pgsql_table_name))
            pg.truncate_table(pg_engine, pgsql_table_name)
    else:
        sql_create_table = ms.get_postgresql_create_sql(ms_engine, mssql_table_name, pgsql_table_name)
        pg.action_query(pg_engine, sql_create_table)
    pg_num_rows = pg.get_count(pg_engine, pgsql_table_name)
    print("### PGSQL {dst} contains {num} rows BEFORE SYNC #####".format(dst=pgsql_table_name,
                                                                         num=pg_num_rows))
    output_file = open(output_filename, mode="w", encoding="utf-8")
    count = 0
    total = 0
    limit = NUM_ROWS_IN_BUFFER
    regex = re.compile(r'\\', flags=re.IGNORECASE)
    while 1:
        row = ms_cursor.fetchone()
        if not row:
            break
        count += 1
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
        output_file.write(row_line)
        data.write(row_line)
        if count >= limit:
            total += count
            count = 0
            print("### PGSQL flushing data to {dst} at {num} rows #####".format(num=total, dst=pgsql_table_name))
            # let's flush content
            data.seek(0)
            if pg.bulk_copy(pg_engine, data, pgsql_table_name, FIELD_DELIMITER):
                data.truncate(0)
                data.seek(0)
                output_file.flush()
            else:
                exit("### PGSQL did have problems trying to import this data")

    # print(data.getvalue())
    data.seek(0)
    pg.bulk_copy(pg_engine, data, pgsql_table_name, FIELD_DELIMITER)
    output_file.close()
    print("### MSSQL {src} contains {num} rows #####".format(src=mssql_table_name,
                                                             num=ms_num_rows_origin))
    pg_num_rows_destination = pg.get_count(pg_engine, pgsql_table_name)
    print("### PGSQL {dst} contains {num} rows AFTER SYNC #####".format(dst=pgsql_table_name,
                                                                        num=pg_num_rows_destination))
    if pg_num_rows_destination < ms_num_rows_origin:
        print("### PGSQL WARNING {dst} missed {num} rows from original data in MSSQL AFTER SYNC #####".format(
            dst=pgsql_table_name,
            num=pg_num_rows_destination - ms_num_rows_origin))

    copy_cmd = "COPY {table} FROM '{file}' WITH DELIMITER AS e{fs!r} NULL AS '{null}' ".format(
        table=pgsql_table_name,
        file=output_filename,
        fs=FIELD_DELIMITER,
        null="\\N")

    # COPY table_name FROM '/tmp/table_name.sql' WITH DELIMITER AS e'\x1f' NULL AS '\N';
    print("### PGSQL a copy of the data imported as been saved in {file}".format(file=output_filename))
    print("### PGSQL you can reload this data from inside psql with :")
    print(copy_cmd)
