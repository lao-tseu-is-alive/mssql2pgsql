#!/usr/bin/python3
# -*- coding: utf-8 -*-
import datetime
import pyodbc
import re
import urllib

import sqlalchemy as sa
import sqlalchemy.engine.reflection
import sqlalchemy.exc
import sqlalchemy.sql.schema

from config import config_goeland_mssql as config

_cached_tables_list = None


def get_engine():
    """ will return a valid SqlAlchemy engine"""
    # need to urlquote password because if your password contains some exotic chars like say @ your dead...
    sqlalchemy_connection = "mssql+pyodbc://" + config.my_user + ":" \
                            + urllib.parse.quote(config.my_password) \
                            + "@" + config.my_dsn
    # + '?charset=utf8'
    # deprecate_large_types=True may be useful for NVARCHAR('max') in MSSQL > 2012
    return sa.create_engine(sqlalchemy_connection, echo=False,
                            connect_args={'convert_unicode': True},

                            legacy_schema_aliasing=False
                            )


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


def query_filter(sql):
    sql_clean = sql.strip()
    bad_sql = {'--', 'DROP', 'TRUNCATE', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'sysobjects'}
    remove = '|'.join(bad_sql)
    regex = re.compile(r'\b(' + remove + r')\b', flags=re.IGNORECASE)
    sql_clean = regex.sub("", sql_clean)
    sql_clean = sql_clean.replace(';', '')
    sql_clean = sql_clean.replace('--', '')
    return sql_clean


def standard_table_names(table_name):
    pure_table_name = table_name.replace("AGF", "Agf")
    pure_table_name = pure_table_name.replace("EGID", "Egid")
    pure_table_name = pure_table_name.replace("ARCH", "Arch")
    pure_table_name = pure_table_name.replace("ASST", "Asst")
    pure_table_name = pure_table_name.replace("CID", "Cid")
    pure_table_name = pure_table_name.replace("CIL", "Cil")
    pure_table_name = pure_table_name.replace("ESTRID", "Estrid")
    pure_table_name = pure_table_name.replace("ES", "Es")
    pure_table_name = pure_table_name.replace("GC", "Gc")
    pure_table_name = pure_table_name.replace("GEO", "Geo")
    pure_table_name = pure_table_name.replace("CCJP", "CcJp")
    pure_table_name = pure_table_name.replace("CH", "Ch")
    pure_table_name = pure_table_name.replace("CN", "Cn")
    pure_table_name = pure_table_name.replace("RCB", "Rcb")
    pure_table_name = pure_table_name.replace("DMZ", "Dmz")
    pure_table_name = pure_table_name.replace("ISO", "Iso")
    pure_table_name = pure_table_name.replace("NT", "Nt")
    pure_table_name = pure_table_name.replace("OPC", "Opc")
    pure_table_name = pure_table_name.replace("POLC", "Polc")
    pure_table_name = pure_table_name.replace("QSE", "Qse")
    pure_table_name = pure_table_name.replace("RM", "Rm")
    pure_table_name = pure_table_name.replace("SAP", "Sap")
    pure_table_name = pure_table_name.replace("SCC", "Scc")
    pure_table_name = pure_table_name.replace("SPD", "Spd")
    pure_table_name = pure_table_name.replace("URB", "Urb")
    return pure_table_name


def convert_to_snake_case(the_camel_case_string):
    return re.sub('(?!^)([A-Z]+)', r'_\1', standard_table_names(the_camel_case_string)).lower()


def query(ms_engine, sql, print_header=True, result_format="text"):
    """
    will execute the sql query against the database defined in config and print the result to standard output
    :param sql: string with sql query, internally will clean any by removing -- ;INSERT,UPDATE,DROP etc..
    :param print_header: boolean (default True) defining if you want column name header displayed
    :param result_format: string (default text) defining the format of output actually only text is supported
    :return: boolean False if something went wrong
    """
    field_max_width = 80
    my_cursor = get_cursor(ms_engine)
    if my_cursor:
        try:
            print("## MSSQL query :{0}".format(query_filter(sql)))
            my_cursor.execute(query_filter(sql))
            row = my_cursor.fetchone()
            if row:
                if print_header:
                    header = ""
                    for field_name, field_type, bof0, field_width, larg2, bof1, bof2 in row.cursor_description:
                        if field_width > field_max_width:
                            field_width = field_max_width
                        if result_format == "text":
                            header += "[{0: ^{field_width}}]".format(field_name, field_width=field_width)
                    print(header)
            while 1:
                if not row:
                    break
                if result_format == "text":
                    row_result = ""
                    for i in range(0, len(row)):
                        field_width = row.cursor_description[i][3]
                        if field_width > field_max_width:
                            field_width = field_max_width
                        if row[i] is None:
                            row_result += "[ NULL ]"
                        else:
                            if type(row[i]) is datetime.datetime:
                                field_string = '[  {0:%Y-%m-%d %H:%M:%S}  ]'.format(row[i], field_width=field_width)
                            else:
                                if len(str(row[i])) > field_max_width:
                                    field_value = str(row[i])[0:(field_max_width - 3)] + "..."
                                else:
                                    field_value = str(row[i])
                                field_string = "[{0: ^{field_width}}]".format(field_value, field_width=field_width)
                            row_result += field_string

                    print(row_result)

                row = my_cursor.fetchone()
        except pyodbc.ProgrammingError as e:
            print("## MSSQL ERROR ## inside query() while executing sql \n{0}".format(sql))
            print(e)
            return False
        except UnicodeDecodeError as e:
            print("## MSSQL UNICODE ERROR ## inside query() while executing sql \n{0}\n".format(sql))
            print(e)
            return False
        finally:
            return True


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


def does_table_exist(ms_engine, tablename, ms_schema='dbo'):
    """ to know if table exist in database """
    return tablename in get_tables_list(ms_engine, ms_schema)


def get_pgsqltype_from_mssql(col):
    # with MSSQL 2012 there is a bug handling NVARCHAR('max')
    if type(col.type) == sa.sql.sqltypes.NVARCHAR:
        return "text"
    if type(col) == sa.sql.schema.Column:
        ctype = str(col.type)
    else:
        ctype = str(col)

    if ctype in ("INTEGER", "TINYINT", "SMALLINT"):
        return "integer"
    elif ctype == "BIGINT":
        return "bigint"
    elif ctype == "BIT":
        return "boolean"
    elif ctype[:7] == "VARCHAR":
        return "text"
    elif ctype[:8] == "NVARCHAR":
        return "text"
    elif ctype[:5] == "NCHAR":
        return "text"
    elif ctype[:4] == "TEXT":
        return "text"
    elif ctype[:5] == "NTEXT":
        return "text"
    elif ctype[:6] == "BINARY":
        # return "bytea({l})".format(l=col.type.length)
        return "bytea"
    elif ctype[:9] == "VARBINARY":
        return "bytea"
    elif ctype[:4] == "CHAR":
        return "char({l})".format(l=col.type.length)
    elif ctype in ("SMALLDATETIME", "DATETIME"):
        return "timestamp"
    elif ctype in ("SMALLMONEY", "MONEY"):
        return "money"
    elif ctype == "UNIQUEIDENTIFIER":
        return "uuid"
    # MSSQL timestamp data type is not the same as the timestamp data type defined in the SQL-92 standard.
    # MSSQL timestamp is used typically as a mechanism for version-stamping table rows. The storage size is 8 bytes.
    # https://technet.microsoft.com/en-us/library/aa260631%28v=sql.80%29.aspx
    # that's why i convert it to a postgresql bigint
    # failing to do so using a simple convert without a cast will raise an error :
    # invalid byte sequence for encoding "UTF8": 0x00
    elif ctype == "TIMESTAMP":
        return "bigint"
    else:
        return ctype


def get_flask_restful_type_from_mssql(col, public_column_name=None):
    if col.nullable:
        null_value = ", default= None"
    else:
        null_value = ""

    if public_column_name is None:
        prefix = "'{col_name}' : ".format(col_name=convert_to_snake_case(col.name))
    else:
        prefix = "'{col_name}' : ".format(col_name=public_column_name)

    suffix = "attribute='{private_name}'{null})".format(private_name=col.name, null=null_value)

    if type(col.type) == sa.sql.sqltypes.NVARCHAR:
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    if type(col) == sa.sql.schema.Column:
        ctype = str(col.type).upper()
    else:
        ctype = str(col).upper()

    if ctype in ("INTEGER", "TINYINT", "SMALLINT", "BIGINT"):
        return "{prefix} fields.Integer({suffix}".format(prefix=prefix, suffix=suffix)
    if ctype in ("REAL", "FLOAT"):
        return "{prefix} fields.Float({suffix}".format(prefix=prefix, suffix=suffix)
    if ctype in ("SMALLMONEY", "MONEY",):
        return "{prefix} fields.Float({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype == "BIT":
        return "{prefix} fields.Boolean({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype[:7] == "VARCHAR":
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype[:8] == "NVARCHAR":
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype[:5] == "NCHAR":
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype[:4] == "TEXT":
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype[:5] == "NTEXT":
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype[:6] == "BINARY":  # should be converted in hex
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype[:9] == "VARBINARY":
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype[:4] == "CHAR":
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype in ("SMALLDATETIME", "DATETIME"):
        return "{prefix} fields.DateTime(dt_format='iso8601',{suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype[:7] in ("DECIMAL", "NUMERIC"):
        # return "{prefix} fields.Fixed(decimals={decim},
        # {suffix}".format(decim=col.type.precision, prefix=prefix, suffix=suffix)
        return "{prefix} fields.Arbitrary({suffix}".format(prefix=prefix, suffix=suffix)
    elif ctype == "UNIQUEIDENTIFIER":
        return "uuid"
    elif ctype == "TIMESTAMP":
        return "{prefix} fields.Integer({suffix}".format(prefix=prefix, suffix=suffix)
    else:
        return "{prefix} fields.Raw({suffix}".format(prefix=prefix, suffix=suffix)


def get_flask_restful_definition_from_mssql(ms_engine, mssql_table_name, table_name):
    """
    get the flask_restful fields from table see: http://flask-restful-cn.readthedocs.io/en/0.3.5/fields.html
    :param ms_engine:
    :param mssql_table_name:
    :param table_name:
    :return: a string with the fields resources declaration
    """

    table_list = get_tables_list(ms_engine)
    if mssql_table_name in table_list:
        print("--### Found table : {t} in mssql db ".format(t=mssql_table_name))
        sa_table = get_mssql_alchemy_table(ms_engine, mssql_table_name)
        sql_query = "resource_{t}_fields = {{".format(t=table_name)
        arr_cols = []
        for c in sa_table.columns:
            col_name = c.name.lower()
            if c.primary_key:
                col_type = get_flask_restful_type_from_mssql(c, 'id')
            else:
                col_type = get_flask_restful_type_from_mssql(c)

            arr_cols.append("\n\t{type}".format(type=col_type))

        sql_query += ",".join(arr_cols)
        sql_query += "\n}"
        return sql_query

    else:
        print("### ERROR table : {t} NOT FOUND in mssql db ".format(t=mssql_table_name))


def get_mssql_alchemy_table(ms_engine, mssql_table_name):
    meta = sa.MetaData(bind=ms_engine, reflect=False, schema='dbo')
    meta.reflect(bind=ms_engine, only=[mssql_table_name])
    return sa.Table(mssql_table_name, meta, autoLoad=True)


def get_count(ms_engine, mssql_table_name, mssql_where_condition = ''):
    if does_table_exist(ms_engine, mssql_table_name):
        sql_query = 'SELECT COUNT(*) as num FROM ' + mssql_table_name
        if len(mssql_where_condition.strip()) > 3:
            sql_query += " WHERE {condition}".format(condition=mssql_where_condition)
        ms_cursor = ms_engine.execute(sql_query)
        row = ms_cursor.fetchone()
        if not row:
            return None
        else:
            return row.num
    else:
        return 0


def get_postgresql_create_sql(ms_engine, mssql_table_name, pgsql_table_name):
    table_list = get_tables_list(ms_engine)
    if mssql_table_name in table_list:
        print("### MSSQL found table : {t} in mssql db, will build CREATE 4 postgresql ".format(t=mssql_table_name))
        sa_table = get_mssql_alchemy_table(ms_engine, mssql_table_name)
        primary_key = "\n\t CONSTRAINT pk_{t} PRIMARY KEY (".format(t=pgsql_table_name)
        sql_query = "CREATE TABLE {t} (".format(t=pgsql_table_name)
        arr_cols = []
        arr_primary_keys = []
        for c in sa_table.columns:
            col_name = c.name.lower()
            col_type = get_pgsqltype_from_mssql(c)
            col_nullable = '' if c.nullable else 'NOT NULL'
            arr_cols.append("\n\t {name} {type} {isnull}".format(name=col_name,
                                                                 type=col_type,
                                                                 isnull=col_nullable))
            if c.primary_key:
                arr_primary_keys.append(col_name)
        sql_query += ",".join(arr_cols)
        if len(arr_primary_keys) > 0:
            sql_query += "," + primary_key + ",".join(arr_primary_keys) + ")\n)"
        else:
            sql_query += "\n)"
        return sql_query

    else:
        print("### ERROR table : {t} NOT FOUND in mssql db ".format(t=mssql_table_name))


def get_select_for_postgresql(ms_engine, mssql_table_name, mssql_where_condition = ''):
    table_list = get_tables_list(ms_engine)
    if mssql_table_name in table_list:
        print("### MSSQL table : {t} found".format(t=mssql_table_name))
        table = get_mssql_alchemy_table(ms_engine, mssql_table_name)
        sql_query = "SELECT  "
        arr_cols = []
        for c in table.columns:
            col_name = c.name.lower()
            col_type = get_pgsqltype_from_mssql(c)
            if c.nullable:
                if col_type == 'text':
                    arr_cols.append(" [{name}]=COALESCE([{src_name}],'\\N')".format(name=col_name, src_name=c.name))
                elif col_type == 'bigint':
                    arr_cols.append(
                        " [{name}]=COALESCE(CONVERT(VARCHAR(1000),CAST([{src_name}] as bigint)),'\\N')".format(
                            name=col_name, src_name=c.name))
                else:
                    arr_cols.append(
                        " [{name}]=COALESCE(CONVERT(VARCHAR(1000),[{src_name}]),'\\N')".format(name=col_name,
                                                                                               src_name=c.name))
            else:
                if col_type == 'text':
                    arr_cols.append(" [{name}]={src_name}".format(name=col_name, src_name=c.name))
                elif col_type == 'bigint':
                    arr_cols.append(
                        " [{name}]=CONVERT(VARCHAR(1000),CAST([{src_name}] as bigint))".format(name=col_name,
                                                                                               src_name=c.name))
                else:
                    arr_cols.append(
                        " [{name}]=CONVERT(VARCHAR(1000),[{src_name}])".format(name=col_name, src_name=c.name))

        sql_query += ",".join(arr_cols) + " FROM {t} ".format(t=mssql_table_name)
        if len(mssql_where_condition.strip()) > 3:
            sql_query += " WHERE {condition}".format(condition=mssql_where_condition)
        return sql_query
    else:
        print("### ERROR table : {t} NOT FOUND in mssql db ".format(t=mssql_table_name))


def get_dbserver_collation(ms_engine):
    ms_cursor = ms_engine.execute("SELECT CONVERT(VARCHAR,SERVERPROPERTY('Collation')) as encoding")
    row = ms_cursor.fetchone()
    if not row:
        return None
    else:
        return row.encoding


if __name__ == '__main__':
    print("##### MSSQL BEGIN Connecting to DATABASE with pyodbc #####")
    ms_engine = get_engine()
    my_cursor = get_cursor(ms_engine)
    print("##### MSSQL DATABASE VERSION with pyodbc #####")
    my_cursor.execute("SELECT @@version")
    while 1:
        row = my_cursor.fetchone()
        if not row:
            break
        print(row[0])
    print("### MSSQL Executing SQL query to DATABASE with pyodbc ###")
    query(ms_engine, "select TOP 10 IdDocument, DocTitle, datecreated from document ORDER BY IdDocument DESC")
    print("##### MSSQL END of TEST with pyodbc #####")
