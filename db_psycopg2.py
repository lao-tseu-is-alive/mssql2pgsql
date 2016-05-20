import psycopg2

from config import config_pgsql as config


def pgsql_connect():
    try:
        con = psycopg2.connect(database=config.my_dbname)
        cursor = con.cursor()
        cursor.execute('SELECT version()')
        ver = cursor.fetchone()
        print("### PGSQL Connected {version}".format(version=ver))
        return con

        # ,
        # host=config.my_host,
        # port=config.my_port,
        # user=config.my_user,
        # password=config.my_password)
    except (psycopg2.DatabaseError) as e:
        print("## ERROR inserting data to POSTGRESQL DB")
        print(e)


def pgsql_count(pg_conn, tablename):
    cursor = pg_conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM ' + tablename)
    num_rows = cursor.fetchone()
    return num_rows[0]
