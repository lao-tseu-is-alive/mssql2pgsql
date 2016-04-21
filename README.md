# mssql2pgsql
some very basic python3 scripts to copy  a table structure and content 
from Microsoft MSSQL Database inside another Postgres database

if the table does not exist on destination Postgresql it will create it
if the table exists with data it will truncate the table
then the script will bulk copy the data using psycopg2 copy_from 
basically it will pull data from MSSQL and push it inside Postgresql

I used SqlAlchemy reflection to get the structure of MSSQL Tables
and in postgres i decided to keep lowercase for fieldnames letters

I'm using it on my servers maybe it can help someone else do the job
Feel free to adapt the code to your needs
