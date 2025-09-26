import os
import sys


class MSSQLConfig:
    """MSSQL Database Configuration from environment variables"""
    dsn = os.getenv("MSSQL_DSN")
    user = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")


class PGSQLConfig:
    """PostgresSQL Database Configuration from environment variables"""
    host = os.getenv("PGSQL_HOST")
    port = os.getenv("PGSQL_PORT")
    dbname = os.getenv("PGSQL_DBNAME")
    user = os.getenv("PGSQL_USER")
    password = os.getenv("PGSQL_PASSWORD")


# A simple check to ensure all required variables are set
required_vars = [
    MSSQLConfig.dsn, MSSQLConfig.user, MSSQLConfig.password,
    PGSQLConfig.host, PGSQLConfig.port, PGSQLConfig.dbname,
    PGSQLConfig.user, PGSQLConfig.password
]

if not all(required_vars):
    print("Error: Not all required environment variables are set.", file=sys.stderr)
    sys.exit(1)
