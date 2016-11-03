# cassandra-fdw
PostgreSQL Foreign Data Wrapper for Cassandra
## Requirements
* PostgreSQL 9.3+
* PostgreSQL development packages (postgresql-server-dev-9.x)
* Cassandra 2.1+, 2.2+, 3+

## Features
* Foreign schema import
* Full CQL types support
* CQL query optimizations

## How to install
#### install Multicorn
```bash
pgxn install multicorn
```
#### install Cassandra driver and modules
```bash
pip install cassandra-driver
pip install pytz
```
#### clone repository
```bash
git clone https://github.com/rankactive/cassandra-fdw.git
```
#### install FDW
```bash
cd cassandra-fdw
python setup.py install
```

## Usage
```SQL
-- Create test database
CREATE DATABASE fdw_test;
```
Switch to database fdw_test
```SQL
-- Create extension for database
CREATE EXTENSION multicorn;
```
```SQL
-- Create server
CREATE SERVER fdw_server FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'cassandra-fdw.CassandraFDW',
  hosts '10.10.10.1,10.10.10.2',
  port '9042',
  username 'cassandra user', -- optional
  password 'cassandra password' -- optional
);
```
```SQL
-- Create foreign table
CREATE FOREIGN TABLE fdw_table
(
  col1 text,
  col2 int,
  col3 bigint
) SERVER fdw_server OPTIONS (keyspace 'cassandra keyspace', columnfamily 'cassandra columnfamily');
```

Use it!
```SQL
SELECT * FROM fdw_table WHERE col1 = 'some text';
```

```SQL
INSERT INTO fdw_table VALUES ('text', 123, 1234);
```

 By default, the concurrency level of modifications is 4. It means that batch modifications will be sent in 4 threads to Cassandra. To change it use:
```SQL
ALTER SERVER fdw_srv OPTIONS (modify_concurency 'your integer value');
```
Or if it has been set before, use:
```SQL
ALTER SERVER fdw_srv OPTIONS (SET modify_concurency 'your integer value');
```

If you want to use updates and deletes, you must create column named "\_\_rowid\_\_" with type "TEXT"

Import foreign schema example:
```SQL
CREATE SCHEMA fdw_test;
IMPORT FOREIGN SCHEMA cassandra_keyspace FROM SERVER fdw_server INTO fdw_test;
```

## Types mapping

| CQL type | PostgreSQL type |
| --- | --- |
| bigint | bigint |
| blob | bytea |
| boolean | boolean |
| counter | bigint |
| date | date |
| decimal | decimal |
| double | float8 |
| float | float4 |
| inet | inet |
| int | int |
| list\<type\> | type[] |
| map\<type, type\> | json |
| set\<type\> | type[] |
| smallint | smallint |
| text | text |
| time | timetz |
| timestamp | timestamptz |
| timeuuid | uuid |
| tinyint | smallint |
| tuple\<type,type,...\> | json |
| uuid | uuid |
| varint | int |
