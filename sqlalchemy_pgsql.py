#!/usr/bin/python3
# -*- coding: utf-8 -*-
import sqlalchemy as sa
import sqlalchemy.exc
import config_pgsql as config
import urllib


def get_engine():
    # need to urlquote password because if your password contains some exotic chars like say @ your dead...
    sqlalchemy_connection = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}".format(
        user=config.my_user, password=urllib.parse.quote(config.my_password), host=config.my_host, port=config.my_port,
        dbname=config.my_dbname)
    # print(sqlalchemy_connection)
    return sa.create_engine(sqlalchemy_connection, echo=False)


def get_cursor(pg_engine):
    """ get a DB-API cursor but don't forget to close() to release it to pool"""
    return pg_engine.raw_connection().cursor()


def bulk_copy(pg_engine, data, pgsql_table_name, field_separator):
    connection = pg_engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.copy_from(data, pgsql_table_name, sep=field_separator)
        cursor.close()
        connection.commit()
    except (sa.exc.SQLAlchemyError, sa.exc.DBAPIError) as e:
        print("## ERROR PGSQL bulk_copy ")
        print("## ERROR PGSQL bulk_copy for table : {table}".format(table=pgsql_table_name))
        print(e)
    finally:
        connection.close()


def action_query(pg_engine, pgsql_action_query):
    # http://docs.sqlalchemy.org/en/rel_1_0/core/connections.html#understanding-autocommit
    connection = pg_engine.connect()
    try:
        connection.execute(sa.sql.expression.text(pgsql_action_query).execution_options(autocommit=True))
    except sa.exc.SQLAlchemyError as e:
        print("## ERROR PGSQL action_query ")
        print("Action query was : {sql}".format(sql=pgsql_action_query))
        print(e)
    finally:
        connection.close()


def get_tables_list(pg_engine, pg_schema='public'):
    insp = sa.engine.reflection.Inspector.from_engine(pg_engine)
    return insp.get_table_names(schema=pg_schema)


def get_table(pg_engine, pgsql_table_name, pg_schema='public'):
    meta = sa.MetaData(bind=pg_engine, reflect=False, schema=pg_schema)
    meta.reflect(bind=pg_engine, only=[pgsql_table_name])
    return sa.Table(pgsql_table_name, meta, autoLoad=True)


def get_count(pg_engine, pgsql_table_name):
    ms_cursor = pg_engine.execute('SELECT COUNT(*) as num FROM ' + pgsql_table_name)
    row = ms_cursor.fetchone()
    if not row:
        return None
    else:
        return row.num


def does_table_exist(pg_engine, pgsql_table_name, pg_schema='public'):
    return pgsql_table_name in get_tables_list(pg_engine, pg_schema)


def escape_copy_string(s):
    #s = s.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    s = s.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return s