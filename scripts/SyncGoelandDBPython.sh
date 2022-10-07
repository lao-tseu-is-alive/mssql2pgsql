#!/bin/bash
LOG="/root/log/SyncGoelandDBPython.log"
echo "*****************************************" >> ${LOG} 2>&1
echo "******** BEGIN  sync of goeland  data at" `date` >> ${LOG} 2>&1
cd /root/bin/mssql2pgsql
#python3 copy_Mssql_Table_to_Postgresql.py DicoCPRueLS >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py LienArbreEspeceCultivar >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py LienArbreGenreEspece >> ${LOG} 2>&1
#python3 copy_Mssql_Table_to_Postgresql.py Parcelle  >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ParcelleDicoType >> ${LOG} 2>&1
#python3 copy_Mssql_Table_to_Postgresql.py ThiArbre >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiArbreCultivar >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiArbreDiamCouronne >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiArbreEspece >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiArbreGenre >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiArbreHauteur >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiArbreValidation >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiBuilding >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiBuildingBatPrincipal >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiBuildingEGID >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiBuildingNoECA >> ${LOG} 2>&1
#python3 copy_Mssql_Table_to_Postgresql.py ThingPosition >> ${LOG} 2>&1
#python3 copy_Mssql_Table_to_Postgresql.py Thing >> ${LOG} 2>&1
#python3 copy_Mssql_Table_to_Postgresql.py ThiSondageGeoTherm >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py ThiStreet >> ${LOG} 2>&1
#python3 copy_Mssql_Table_to_Postgresql.py ThiStreetBuildingAddress >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py TypeThing >> ${LOG} 2>&1
python3 copy_Mssql_Table_to_Postgresql.py TypeThiStreet >> ${LOG} 2>&1
su -c "psql -c 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO readers;' goeland" postgres  >> ${LOG} 2>&1
echo "******** END sync of goeland  data at" `date` >> ${LOG} 2>&1
echo "*****************************************" >> ${LOG} 2>&1
