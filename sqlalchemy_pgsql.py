#!/usr/bin/python3
# -*- coding: utf-8 -*-
import sys
import urllib
from urllib.parse import quote
import psycopg2
import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy import text

from config import PGSQLConfig as confPG


def get_engine():
    """
   Returns a valid SqlAlchemy engine and tests the connection.
   Fails early if the database is not available.
   """
    try:
        sqlalchemy_connection = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}".format(
            user=confPG.user, password=urllib.parse.quote(confPG.password),
            host=confPG.host, port=confPG.port,
            dbname=confPG.dbname)
        db = sa.create_engine(sqlalchemy_connection, echo=False)
        # -- Fail Early Connection Test --
        # Try to establish a connection to verify credentials and server availability.
        # print("mssql> Testing database connection...")
        with db.connect() as connection:
            print("✅ pgsql> Connection successful.")

        return db

    except sa.exc.OperationalError as e:
        print(f"❌ pgsql> ERROR: Database connection failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ pgsql> ERROR: An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)



def get_cursor(pg_engine):
    """ get a DB-API cursor but don't forget to close() to release it to pool"""
    return pg_engine.raw_connection().cursor()


def bulk_copy(pg_engine, data, pgsql_table_name, field_separator):
    data.seek(0)
    connection = pg_engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.copy_from(data, pgsql_table_name, sep=field_separator)

        cursor.close()
        connection.commit()
        return True
    except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError, psycopg2.DataError) as e:
        print("## ERROR PGSQL bulk_copy ")
        print(data.getvalue())
        print("## ERROR PGSQL bulk_copy for table : {table}".format(table=pgsql_table_name))
        print(e)
        return False

    finally:
        connection.close()


def truncate_table(pg_engine, pgsql_table_name):
    connection = pg_engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE {table} RESTART IDENTITY;".format(table=pgsql_table_name))
            cursor.close()
        connection.commit()
        connection.close()
    except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError, psycopg2.DataError) as e:
        print("## ERROR PGSQL truncate_table ")
        print("## ERROR PGSQL truncate_table for table : {table}".format(table=pgsql_table_name))
        print(e)
    finally:
        connection.close()


def action_query(pg_engine, pgsql_action_query):
    # http://docs.sqlalchemy.org/en/rel_1_0/core/connections.html#understanding-autocommit
    connection = pg_engine.connect()
    try:
        connection.execute(text(pgsql_action_query))
        connection.commit()
    except sa.exc.SQLAlchemyError as e:
        print("## ERROR PGSQL action_query ")
        print("Action query was : {sql}".format(sql=pgsql_action_query))
        print(e)
    finally:
        connection.close()


def get_tables_list(pg_engine, pg_schema='public'):
    insp = sa.engine.reflection.Inspector.from_engine(pg_engine)
    return insp.get_table_names(schema=pg_schema)


def does_table_exist(pg_engine, pgsql_table_name, pg_schema='public'):
    """ to know if table exist in database """
    return pgsql_table_name in get_tables_list(pg_engine, pg_schema)


def get_count(pg_engine, pgsql_table_name):
    """ to get number of records in table """
    if does_table_exist(pg_engine, pgsql_table_name):
        with pg_engine.connect() as connection:
            cursor = connection.execute(text('SELECT COUNT(*) as num FROM ' + pgsql_table_name))
            row = cursor.fetchone()
            if not row:
                return None
            else:
                return row.num
    else:
        return 0


def get_dbserver_encoding(pg_engine):
    with pg_engine.connect() as connection:
        cursor = connection.execute(text('SHOW SERVER_ENCODING;'))
        row = cursor.fetchone()
        if not row:
            return None
        else:
            return row[0]


def get_dbclient_encoding(pg_engine):
    with pg_engine.connect() as connection:
        cursor = connection.execute(text('SHOW CLIENT_ENCODING;'))
        row = cursor.fetchone()
        if not row:
            return None
        else:
            return row[0]
