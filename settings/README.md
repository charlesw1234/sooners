For devel settings
==================

To run the test with devel settings, the following databases have to be setupped.

For PostgreSQL
--------------

To setup PostgreSQL user and database:
```bash
$ sudo -u postgres psql
```
```sql
postgres=# CREATE USER sooners WITH PASSWORD '$abc123def$';
postgres=# CREATE DATABASE sooners WITH ONER sooners TEMPLATE template0;
postgres=# GRANT ALL ON DATABASE sooners TO sooners;
```

To drop PostgreSQL user and databases:
```bash
$ sudo -u postgres psql
```
```sql
postgres=# DROP DATABASE sooners;
postgres=# DROP USER sooners;
```

For MySQL
---------

To setup MySQL user and database:
```bash
$ sudo mysql -u root -p
```
```sql
mysql> CREATE USER 'sooners'@'localhost' IDENTIFIED BY '$abc123def$';
mysql> CREATE DATABASE IF NOT EXISTS sooners;
mysql> GRANT ALL PRIVILEGES ON sooners.* TO 'sooners'@'localhost';
```

To drop MySQL user and database:
```bash
$ sudo mysql -u root -p
```
```sql
mysql> DROP DATABASE sooners;
mysql> DROP USER sooners;
```

For SQlite3
-----------

As tested, sqlite-3.7.2 which used as default version by Ubuntu22.04 is required.
