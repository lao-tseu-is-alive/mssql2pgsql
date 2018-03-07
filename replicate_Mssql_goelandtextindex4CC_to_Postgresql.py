#!/usr/bin/python3
# -*- coding: utf-8 -*-
from io import StringIO
import re
import sys
import sqlalchemy_mssql as ms
import sqlalchemy_pgsql as pg
NUM_ROWS_IN_BUFFER = 2000

mssql_table_name = "goelandtextindex"
mssql_where_condition = " is4extranetcc = 1"

# using ascii code 31 (unit separator)
FIELD_DELIMITER = u""+chr(31)
# in Postgresql you can choose to keep mixed case characters
# but be careful you will need to quote the table like this  SELECT * FROM "YourMixedCaseTableName"
pgsql_table_name = ms.convert_to_snake_case(mssql_table_name)
output_filename = "/tmp/" + pgsql_table_name + ".sql"
print("##### BEGIN SYNC MSSQL {src} with PGSQL {dst}#####".format(src=mssql_table_name,
                                                                  dst=pgsql_table_name))
ms_engine = ms.get_engine()
ms_num_rows_origin = ms.get_count(ms_engine, mssql_table_name, mssql_where_condition)
print("### MSSQL {src} contains {num} rows #####".format(src=mssql_table_name,
                                                         num=ms_num_rows_origin))
sql_query = ms.get_select_for_postgresql(ms_engine, mssql_table_name, mssql_where_condition)
print("### ABOUT TO RUN SQL QUERY :\n", sql_query)
# print("### BEFORE \n")
# ms.query(ms_engine, sql_query)
# print("### NEXT \n")
ms_cursor = ms_engine.execute(sql_query)
print("### MSSQL DB SERVER COLLATION : {enc}".format(enc=ms.get_dbserver_collation(ms_engine)))
data = StringIO()
pg_engine = pg.get_engine()
# TODO if postgres dbserver encoding is not utf-8 do the convert to right encoding
print("### PGSQL DB SERVER ENCODING : {enc}".format(enc=pg.get_dbserver_encoding(pg_engine)))
print("### PGSQL DB CLIENT ENCODING : {enc}".format(enc=pg.get_dbclient_encoding(pg_engine)))
pg_num_rows = 0
# here we drop table and recreate in postgresql because we then add the text_index tsvector field
print("### PGSQL  will DROP TABLE {dst} BEFORE SYNC #####".format(dst=pgsql_table_name))
pg.action_query(pg_engine, "DROP TABLE {dst};".format(dst=pgsql_table_name))
sql_create_table = ms.get_postgresql_create_sql(ms_engine, mssql_table_name, pgsql_table_name)
print("### PGSQL  will CREATE TABLE {dst} BEFORE SYNC #####".format(dst=pgsql_table_name))
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
        num=pg_num_rows_destination-ms_num_rows_origin))

copy_cmd = "COPY {table} FROM '{file}' WITH DELIMITER AS e{fs!r} NULL AS '{null}' ".format(
    table=pgsql_table_name,
    file=output_filename,
    fs=FIELD_DELIMITER,
    null="\\N")

# COPY table_name FROM '/tmp/table_name.sql' WITH DELIMITER AS e'\x1f' NULL AS '\N';
print("### PGSQL a copy of the data imported as been saved in {file}".format(file=output_filename))
print("### PGSQL you can reload this data from inside psql with :")
print(copy_cmd)

print("### PGSQL will create unique index on table {dst} iddomain, idgoeland with #####".format(dst=pgsql_table_name))
sql_query = """
CREATE UNIQUE INDEX goelandtextindex_unique_iddomainidgoeland  
ON goelandtextindex 
USING btree (iddomain, idgoeland) WITH (FILLFACTOR=95);
"""
pg.action_query(pg_engine, sql_query)
pg.action_query(pg_engine, 'ALTER TABLE goelandtextindex CLUSTER ON goelandtextindex_unique_iddomainidgoeland;')

print("### PGSQL will add field text_index on table {dst} #####".format(dst=pgsql_table_name))
pg.action_query(pg_engine, 'ALTER TABLE goelandtextindex ADD COLUMN text_index tsvector;')

print("### PGSQL will update field text_index on table #####".format(dst=pgsql_table_name))
sql_query = """
UPDATE goelandtextindex SET 
  text_index = setweight(to_tsvector('french',coalesce(unaccent(nom),'')),'A') 
            || setweight(to_tsvector('french',coalesce(unaccent(description),'')),'B') 
            || setweight(to_tsvector('french',coalesce(unaccent(commentaire),'')),'C') ;
"""
pg.action_query(pg_engine, sql_query)
print("### PGSQL will create gin index on field text_index for table #####".format(dst=pgsql_table_name))
pg.action_query(pg_engine,
                'CREATE INDEX idx_goelandtextindex_gin_text_index ON goelandtextindex USING gin(text_index);')

pg.action_query(pg_engine, 'GRANT SELECT ON goelandtextindex TO cgdbreadonly;')
pg.action_query(pg_engine, 'GRANT SELECT ON doc_family TO cgdbreadonly;')
pg.action_query(pg_engine, 'GRANT SELECT ON docgroup TO cgdbreadonly;')
pg.action_query(pg_engine, 'GRANT SELECT ON goelanddomain TO cgdbreadonly;')
pg.action_query(pg_engine, 'GRANT ALL ON goelandtextindex TO cgdb;')
pg.action_query(pg_engine, 'GRANT ALL ON doc_family TO cgdb;')
pg.action_query(pg_engine, 'GRANT SELECT ON docgroup TO cgdbreadonly;')
pg.action_query(pg_engine, 'GRANT SELECT ON goelanddomain TO cgdbreadonly;')