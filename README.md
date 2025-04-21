connect to postgresql
sudo service postgresql start
sudo -i -u postgres
psql
ALTER USER ketan WITH PASSWORD 'Arti@1982'
CREATE DATABASE nosql;
connect to hive
hive --service hiveserver2
connect to mongo
for mongo, we are using mongodb atlas, connect there
establish new connection, then create database user and then use the code to connect to the mongodb user