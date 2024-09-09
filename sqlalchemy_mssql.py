#!/usr/bin/python3
# -*- coding: utf-8 -*-
import datetime
import re
from urllib.parse import quote

import pyodbc
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.sql.schema
from sqlalchemy import text
from sqlalchemy.sql import sqltypes

from config import config_goeland_mssql as config

_cached_tables_list = None


def get_engine():
    """ will return a valid SqlAlchemy engine"""
    # need to urlquote password because if your password contains some exotic chars like say @ your dead...
    sqlalchemy_connection = "mssql+pyodbc://" + config.my_user + ":" \
                            + quote(config.my_password) \
                            + "@" + config.my_dsn
    # + '?charset=utf8'
    # deprecate_large_types=True may be useful for NVARCHAR('max') in MSSQL > 2012
    return sa.create_engine(sqlalchemy_connection, echo=False,
                            connect_args={'convert_unicode': True},

                            legacy_schema_aliasing=False
                            )


def get_cursor(alchemy_engine):
    """ to get a DB-API cursor but don't forget to close() to release it to pool"""
    return alchemy_engine.raw_connection().cursor()


def action_query(alchemy_engine, ddl_query):
    """ to run a one shot query like a TRUNCATE a CREATE or whatever query that does not return a recordset"""
    # http://docs.sqlalchemy.org/en/rel_1_0/core/connections.html#understanding-autocommit
    with alchemy_engine.connect() as connection:
        with connection.begin():
            try:
                connection.execute(text(ddl_query))
            except sa.exc.SQLAlchemyError as e:
                print("## ERROR MSSQL query ")
                print("Action query was : {sql}".format(sql=ddl_query))
                print(e)


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


def query(alchemy_engine, sql, print_header=True, result_format="text"):
    """
    will execute the sql query against the database defined in config and print the result to standard output
    :param alchemy_engine: a valid sqlalchemy engine
    :param sql: string with sql query, internally will clean any by removing -- ;INSERT,UPDATE,DROP etc..
    :param print_header: boolean (default True) defining if you want column name header displayed
    :param result_format: string (default text) defining the format of output actually only text is supported
    :return: boolean False if something went wrong
    """
    field_max_width = 80
    cursor = get_cursor(alchemy_engine)
    if cursor:
        try:
            print("## MSSQL query :{0}".format(query_filter(sql)))
            cursor.execute(query_filter(sql))
            record = cursor.fetchone()
            if record:
                if print_header:
                    header = ""
                    for field_name, field_type, bof0, field_width, larg2, bof1, bof2 in record.cursor_description:
                        if field_width > field_max_width:
                            field_width = field_max_width
                        if result_format == "text":
                            header += "[{0: ^{field_width}}]".format(field_name, field_width=field_width)
                    print(header)
            while 1:
                if not record:
                    break
                if result_format == "text":
                    row_result = ""
                    for i in range(0, len(record)):
                        field_width = record.cursor_description[i][3]
                        if field_width > field_max_width:
                            field_width = field_max_width
                        if record[i] is None:
                            row_result += "[ NULL ]"
                        else:
                            if type(record[i]) is datetime.datetime:
                                field_string = '[  {0:%Y-%m-%d %H:%M:%S}  ]'.format(record[i], field_width=field_width)
                            else:
                                if len(str(record[i])) > field_max_width:
                                    field_value = str(record[i])[0:(field_max_width - 3)] + "..."
                                else:
                                    field_value = str(record[i])
                                field_string = "[{0: ^{field_width}}]".format(field_value, field_width=field_width)
                            row_result += field_string

                    print(row_result)

                record = cursor.fetchone()
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


def get_tables_list(alchemy_engine, ms_schema='dbo'):
    """ to get the list of existing tables in a specific schema or in the default schema """

    # next line is VERY VERY long to run it loads all tables definition with FK etc...
    # meta = sa.MetaData(bind=engine, reflect=False, schema='dbo')
    # meta.reflect(bind=engine)
    # for table in meta.sorted_tables:
    #    print(table.name, table.columns)
    # insp = sa.engine.reflection.Inspector.from_engine(engine)

    inspector = sa.engine.reflection.Inspector.from_engine(alchemy_engine)
    return inspector.get_table_names(schema=ms_schema)


def does_table_exist(alchemy_engine, tablename, ms_schema='dbo'):
    """ to know if table exist in database """
    return tablename in get_tables_list(alchemy_engine, ms_schema)


def get_pgsqltype_from_mssql(col):
    # with MSSQL 2012 there is a bug handling NVARCHAR('max')
    if type(col['type']) is sa.sql.sqltypes.NVARCHAR:
        return "text"
    if type(col) is sa.sql.schema.Column:
        ctype = str(col['type'])
    else:
        ctype = str(col['type'])

    if ctype in ("INTEGER", "TINYINT", "SMALLINT", "INTEGER()"):
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
        m = re.match(r"CHAR\((\d+)\)", ctype)
        char_length = 5
        if m:
            char_length = m.group(1)
        return "char({l})".format(l=char_length)
    elif ctype in ("SMALLDATETIME", "DATETIME"):
        return "timestamp"
    elif ctype in ("SMALLMONEY", "MONEY"):
        return "decimal(20, 4)"
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

    if type(col.type) is sa.sql.sqltypes.NVARCHAR:
        return "{prefix} fields.String({suffix}".format(prefix=prefix, suffix=suffix)
    if type(col) is sa.sql.schema.Column:
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


def get_count(alchemy_engine, mssql_table_name, mssql_where_condition=''):
    if does_table_exist(alchemy_engine, mssql_table_name):
        sql_query = 'SELECT COUNT(*) as num FROM ' + mssql_table_name
        if len(mssql_where_condition.strip()) > 3:
            sql_query += " WHERE {condition}".format(condition=mssql_where_condition)
        with alchemy_engine.connect() as ms_connection:
            ms_cursor = ms_connection.execute(text(sql_query))
            my_record = ms_cursor.fetchone()
            if not my_record:
                return None
            else:
                return my_record.num
    else:
        return 0


def get_postgresql_create_sql(alchemy_engine, mssql_table_name, pgsql_table_name):
    table_list = get_tables_list(alchemy_engine)
    if mssql_table_name in table_list:
        print("### MSSQL found table : {t} in mssql db, will build CREATE 4 postgresql ".format(t=mssql_table_name))
        # sa_table = get_mssql_alchemy_table(engine, mssql_table_name)
        inspect_tool = sa.inspect(alchemy_engine)
        table_columns = inspect_tool.get_columns(mssql_table_name)
        arr_primary_keys_columns = inspect_tool.get_pk_constraint(mssql_table_name)['constrained_columns']
        primary_key = "\n\t CONSTRAINT pk_{t} PRIMARY KEY (".format(t=pgsql_table_name)
        sql_query = "CREATE TABLE {t} (".format(t=pgsql_table_name)
        arr_cols = []
        arr_primary_keys = []
        for c in table_columns:
            col_name = c['name'].lower()
            col_type = get_pgsqltype_from_mssql(c)
            col_nullable = '' if c['nullable'] else 'NOT NULL'
            arr_cols.append("\n\t {name} {type} {isnull}".format(name=col_name,
                                                                 type=col_type,
                                                                 isnull=col_nullable))
            if c['name'] in arr_primary_keys_columns:
                arr_primary_keys.append(col_name)
        sql_query += ",".join(arr_cols)
        if len(arr_primary_keys) > 0:
            sql_query += "," + primary_key + ",".join(arr_primary_keys) + ")\n)"
        else:
            sql_query += "\n)"
        return sql_query

    else:
        print("### ERROR table : {t} NOT FOUND in mssql db ".format(t=mssql_table_name))


def get_select_for_postgresql(alchemy_engine, mssql_table_name, mssql_where_condition=''):
    table_list = get_tables_list(alchemy_engine)
    if mssql_table_name in table_list:
        print("### MSSQL table : {t} found".format(t=mssql_table_name))
        # table = get_mssql_alchemy_table(engine, mssql_table_name)
        inspect_tool = sa.inspect(alchemy_engine)
        table_columns = inspect_tool.get_columns(mssql_table_name)
        sql_query = "SELECT  "
        arr_cols = []
        for c in table_columns:
            col_name = c['name'].lower()
            col_type = get_pgsqltype_from_mssql(c)
            if c['nullable']:
                if col_type == 'text':
                    arr_cols.append(" [{name}]=COALESCE([{src_name}],'\\N')".format(name=col_name, src_name=c['name']))
                elif col_type == 'bigint':
                    arr_cols.append(
                        " [{name}]=COALESCE(CONVERT(VARCHAR(1000),CAST([{src_name}] as bigint)),'\\N')".format(
                            name=col_name, src_name=c['name']))
                elif col_type == 'timestamp':
                    arr_cols.append(
                        " [{name}]=COALESCE(CONVERT(VARCHAR(1000),[{src_name}],21),'\\N')".format(name=col_name,
                                                                                                  src_name=c['name']))
                else:
                    arr_cols.append(
                        " [{name}]=COALESCE(CONVERT(VARCHAR(1000),[{src_name}]),'\\N')".format(name=col_name,
                                                                                               src_name=c['name']))
            else:
                if col_type == 'text':
                    arr_cols.append(" [{name}]={src_name}".format(name=col_name, src_name=c['name']))
                elif col_type == 'bigint':
                    arr_cols.append(
                        " [{name}]=CONVERT(VARCHAR(1000),CAST([{src_name}] as bigint))".format(name=col_name,
                                                                                               src_name=c['name']))
                elif col_type == 'timestamp':
                    arr_cols.append(
                        " [{name}]=CONVERT(VARCHAR(1000),[{src_name}], 21)".format(name=col_name, src_name=c['name']))
                else:
                    arr_cols.append(
                        " [{name}]=CONVERT(VARCHAR(1000),[{src_name}])".format(name=col_name, src_name=c['name']))

        sql_query += ",".join(arr_cols) + " FROM {t} ".format(t=mssql_table_name)
        if len(mssql_where_condition.strip()) > 3:
            sql_query += " WHERE {condition}".format(condition=mssql_where_condition)
        return sql_query
    else:
        print("### ERROR table : {t} NOT FOUND in mssql db ".format(t=mssql_table_name))
        exit(1)


def get_dbserver_collation(alchemy_engine):
    with alchemy_engine.connect() as ms_connection:
        ms_cursor = ms_connection.execute(text("SELECT CONVERT(VARCHAR,SERVERPROPERTY('Collation')) as encoding"))
        record = ms_cursor.fetchone()
        if not record:
            return None
        else:
            return record.encoding


if __name__ == '__main__':
    print("##### MSSQL BEGIN Connecting to DATABASE with pyodbc #####")
    engine = get_engine()
    my_cursor = get_cursor(engine)
    print("##### MSSQL DATABASE VERSION with pyodbc #####")
    my_cursor.execute("SELECT @@version")
    while 1:
        row = my_cursor.fetchone()
        if not row:
            break
        print(row[0])
    print("### MSSQL Executing SQL query to DATABASE with pyodbc ###")
    query(engine, "select TOP 10 IdDocument, DocTitle, datecreated from document ORDER BY IdDocument DESC")
    print("##### MSSQL END of TEST with pyodbc #####")
