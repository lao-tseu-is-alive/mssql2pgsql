#!/usr/bin/python3
# -*- coding: utf-8 -*-
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.sql.schema
import sqlalchemy.engine.reflection
import config_mssql as config
import urllib


def get_engine():
    """ will return a valid SqlAlchemy engine"""
    # need to urlquote password because if your password contains some exotic chars like say @ your dead...
    sqlalchemy_connection = "mssql+pyodbc://" + config.my_user + ":" \
                            + urllib.parse.quote(config.my_password) + "@" + config.my_dsn
    return sa.create_engine(sqlalchemy_connection, echo=False,
                            connect_args={'convert_unicode': True})


def get_cursor(ms_engine):
    """ to get a DB-API cursor but don't forget to close() to release it to pool"""
    return ms_engine.raw_connection().cursor()


def action_query(ms_engine, action_query):
    """ to run a one shot query like a TRUNCATE a CREATE or whatever query that does not return a recordset"""
    # http://docs.sqlalchemy.org/en/rel_1_0/core/connections.html#understanding-autocommit
    connection = ms_engine.connect()
    try:
        connection.execute(sa.sql.expression.text(action_query).execution_options(autocommit=True))
    except sa.exc.SQLAlchemyError as e:
        print("## ERROR PGSQL action_query ")
        print("Action query was : {sql}".format(sql=action_query))
        print(e)
    finally:
        connection.close()


def get_tables_list(ms_engine, ms_schema='dbo'):
    """ to get the list of existing tables in a specific schema or in the default schema """

    # next line is VERY VERY long to run it loads all tables definition with FK etc...
    # meta = sa.MetaData(bind=engine, reflect=False, schema='dbo')
    # meta.reflect(bind=engine)
    # for table in meta.sorted_tables:
    #    print(table.name, table.columns)
    # insp = sa.engine.reflection.Inspector.from_engine(engine)

    inspector = sa.engine.reflection.Inspector.from_engine(ms_engine)
    return inspector.get_table_names(schema=ms_schema)


def get_pgsqltype_from_mssql(col):
    if type(col) == sa.sql.schema.Column:
        ctype = str(col.type)
    else:
        ctype = str(col)

    if ctype == "INTEGER":
        return "integer"
    elif ctype == "BIT":
        return "boolean"
    elif ctype[:7] == "VARCHAR":
        return "text"
    elif ctype[:8] == "NVARCHAR":
        return "text"
    elif ctype[:4] == "CHAR":
        return "char({l})".format(l=col.type.length)
    elif ctype == "DATETIME":
        return "timestamp"
    elif ctype == "UNIQUEIDENTIFIER":
        return "uuid"
    else:
        return ctype


def get_mssql_alchemy_table(ms_engine, mssql_table_name):
    meta = sa.MetaData(bind=ms_engine, reflect=False, schema='dbo')
    meta.reflect(bind=ms_engine, only=[mssql_table_name])
    return sa.Table(mssql_table_name, meta, autoLoad=True)


def get_count(ms_engine, mssql_table_name):
    ms_cursor = ms_engine.execute('SELECT COUNT(*) as num FROM ' + mssql_table_name)
    row = ms_cursor.fetchone()
    if not row:
        return None
    else:
        return row.num


def get_postgresql_create_sql(ms_engine, mssql_table_name, pgsql_table_name):
    table_list = get_tables_list(ms_engine)
    if mssql_table_name in table_list:
        print("--### Found table : {t} in mssql db ".format(t=mssql_table_name))
        sa_table = get_mssql_alchemy_table(ms_engine, mssql_table_name)
        primary_key = "\n\t CONSTRAINT pk_{t} PRIMARY KEY (".format(t=pgsql_table_name)
        sql_query = "CREATE TABLE {t} (".format(t=mssql_table_name.lower())
        arr_cols = []
        for c in sa_table.columns:
            col_name = c.name.lower()
            col_type = get_pgsqltype_from_mssql(c)
            col_nullable = '' if c.nullable else 'NOT NULL'
            arr_cols.append("\n\t {name} {type} {isnull}".format(name=col_name,
                                                                type=col_type,
                                                                isnull=col_nullable))
            primary_key += col_name + ' ' if c.primary_key else ''
        sql_query += ",".join(arr_cols)
        sql_query += "," + primary_key + ")\n)"
        return sql_query

    else:
        print("### ERROR table : {t} NOT FOUND in mssql db ".format(t=mssql_table_name))


def get_select_for_postgresql(ms_engine, mssql_table_name):
    table_list = get_tables_list(ms_engine)
    if mssql_table_name in table_list:
        print("### MSSQL table : {t} found".format(t=mssql_table_name))
        Table = get_mssql_alchemy_table(ms_engine, mssql_table_name)
        sql_query = "SELECT  "
        arr_cols = []
        for c in Table.columns:
            col_name = c.name.lower()
            col_type = get_pgsqltype_from_mssql(c)
            if c.nullable:
                if col_type == 'text':
                    arr_cols.append(" {name}=COALESCE({src_name},'\\N')".format(name=col_name, src_name=c.name))
                else:
                    arr_cols.append(
                        " {name}=COALESCE(CONVERT(VARCHAR,{src_name}),'\\N')".format(name=col_name, src_name=c.name))
            else:
                if col_type == 'text':
                    arr_cols.append(" {name}={src_name}".format(name=col_name, src_name=c.name))
                else:
                    arr_cols.append(" {name}=CONVERT(VARCHAR,{src_name})".format(name=col_name, src_name=c.name))

        sql_query += ",".join(arr_cols) + "FROM {t} ".format(t=mssql_table_name.lower())
        return sql_query
    else:
        print("### ERROR table : {t} NOT FOUND in mssql db ".format(t=mssql_table_name))
