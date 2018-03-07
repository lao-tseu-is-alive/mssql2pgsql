#!/usr/bin/python3
import sys
import os
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, base_dir)
import db_pyodbc as db
import sqlalchemy_mssql as ms
# testing connection to GOELAND MSSQL DB with pyodbc and SqlAlchemy

print("##### MSSQL BEGIN Connecting to DATABASE with pyodbc #####")
print("### MSSQL SQL query with pyodbc ###")
sql = "select TOP 2 IdDocument, DocTitle from document ORDER BY IdDocument DESC"
print("## sql is :", sql)
db.query(sql)
print("##### END of TEST with pyodbc #####")
print("##### BEGIN Connecting to DATABASE with SQLAlchemy pyodbc #####")
engine = ms.get_engine()
print("### Executing SQL query with SQLAlchemy pyodbc ###")
print("## sql is :", sql)
cursor = engine.execute(sql)
print("### Displaying results of SQL query  with SQLALCHEMY       ###")
while 1:
    row = cursor.fetchone()
    if not row:
        break
    print('[', row.IdDocument, ']', row.DocTitle)
print("### Listing Tables with SQLALCHEMY       ###")
table_list = ms.get_tables_list(engine)
#for table in table_list:
#    print(table)