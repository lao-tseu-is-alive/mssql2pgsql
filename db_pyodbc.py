#!/usr/bin/python3
# -*- coding: utf-8 -*-
import datetime
import re
import pyodbc
import config_goeland_mssql as config

__author__ = 'cgil'

# Very small basic functions to test connection to GOELAND MSSQL DB with pyodbc


def get_connection():
    """
    allows to connect to the database defined in config
    :return: a valid connection or false if something went wrong
    """
    try:
        return pyodbc.connect('DSN=' + config.my_dsn + ';UID=' + config.my_user + ';PWD=' + config.my_password)
    except (pyodbc.ProgrammingError, pyodbc.Error) as e:
        print("##ERROR## inside connect() while connecting to DB using dsn {0}".format(config.my_dsn))
        print(e)
        return False


def get_cursor():
    """
        allows to get a pyodbc cursor to the database defined in config
        :return: a valid cursor or false if something went wrong
    """
    connection = get_connection()
    if connection:
        return connection.cursor()
    else:
        return False


def query_filter(sql):
    sql_clean = sql.strip()
    bad_sql = {'--', 'DROP', 'TRUNCATE', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'sysobjects'}
    remove = '|'.join(bad_sql)
    regex = re.compile(r'\b(' + remove + r')\b', flags=re.IGNORECASE)
    sql_clean = regex.sub("", sql_clean)
    sql_clean = sql_clean.replace(';', '')
    sql_clean = sql_clean.replace('--', '')
    return sql_clean


def query(sql, print_header=True, result_format="text"):
    """
    will execute the sql query against the database defined in config and print the result to standard output
    :param sql: string with sql query, internally will clean any by removing -- ;INSERT,UPDATE,DROP etc..
    :param print_header: boolean (default True) defining if you want column name header displayed
    :param result_format: string (default text) defining the format of output actually only text is supported
    :return: boolean False if something went wrong
    """
    field_max_width = 80
    my_cursor = get_cursor()
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
        finally:
            return True


if __name__ == '__main__':
    print("##### MSSQL BEGIN Connecting to DATABASE with pyodbc #####")
    my_cursor = get_cursor()
    print("##### MSSQL DATABASE VERSION with pyodbc #####")
    my_cursor.execute("SELECT @@version")
    while 1:
        row = my_cursor.fetchone()
        if not row:
            break
        print(row[0])
    print("### MSSQL Executing SQL query to DATABASE with pyodbc ###")
    query("select TOP 10 IdDocument, DocTitle, datecreated from document ORDER BY IdDocument DESC")
    print("##### MSSQL END of TEST with pyodbc #####")
