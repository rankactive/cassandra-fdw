from cassandra.metadata import Metadata
from cassandra.cluster import Cluster
from multicorn import TableDefinition, ColumnDefinition
from cassandra.auth import PlainTextAuthProvider
import types_mapper
import logger
from logger import WARNING, ERROR
from properties import ISDEBUG

def import_schema(schema, srv_options, options, restriction_type, restricts):
    if ISDEBUG:
        logger.log(u"import schema {0} requiested with options {1}; restriction type: {2}; restrictions: {3}".format(schema, options, restriction_type, restricts))
    if "hosts" not in srv_options:
        logger.log("The hosts parameter is needed, setting to localhost.", WARNING)
    hosts = srv_options.get("hosts", "localhost").split(",")
    if "port" not in srv_options:
        logger.log("The port parameter is needed, setting to 9042.", WARNING)
    port = srv_options.get("port", "9042")
    username = srv_options.get("username", None)
    password = srv_options.get("password", None)
    with_row_id = options.get('with_row_id', 'True') == 'True'
    names_mapping = options.get('mapping', '').split(';')
    mapping_dict = {}
    mapping_dict_backward = {}
    for s in names_mapping:
        kp = s.split('=')
        if len(kp) != 2:
            continue
        key = kp[0].strip()
        value = kp[1].strip()
        mapping_dict[key] = value
        mapping_dict_backward[value] = key

    cluster = Cluster(hosts)
    if(username is not None):
        cluster.auth_provider = PlainTextAuthProvider(username=username, password=password)
    # Cassandra connection init
    session = cluster.connect()
    keyspace = cluster.metadata.keyspaces[schema]
    cassandra_tables = []
    tables = keyspace.tables
    views = keyspace.views
    if restriction_type is None:
        for t in tables:
            if t in tables:
                cassandra_tables.append(tables[t])
            else:
                cassandra_tables.append(views[t])
    elif restriction_type == 'limit':
        for r in restricts:
            t_name = r 
            if t_name in mapping_dict_backward:
                t_name = mapping_dict_backward[t_name]
            if t_name in tables:
                cassandra_tables.append(tables[t_name])
            else:
                cassandra_tables.append(views[t_name])
    elif restriction_type == 'except':
        for t in tables:
            if t not in restricts:
                if t in tables:
                    cassandra_tables.append(tables[t])
                else:
                    cassandra_tables.append(views[t])
    pg_tables = []
    for c_table in cassandra_tables:
        if ISDEBUG:
            logger.log("Importing table {0}...".format(c_table.name))
        pg_table_name = c_table.name
        if pg_table_name in mapping_dict:
            if ISDEBUG:
                logger.log("Cassandra table name '{0}' maps to PostgreSQL table name '{1}'".format(pg_table_name, mapping_dict[pg_table_name]))
            pg_table_name = mapping_dict[pg_table_name]
        pg_table = TableDefinition(pg_table_name)
        pg_table.options['keyspace'] = schema
        pg_table.options['columnfamily'] = c_table.name
        for c_column_name in c_table.columns:
            cql_type = c_table.columns[c_column_name].cql_type
            pg_type = types_mapper.get_pg_type(cql_type)
            if ISDEBUG:
                logger.log("Adding column {0} with PostgreSQL type {2} (CQL type {1})".format(c_column_name, cql_type, pg_type))
            pg_table.columns.append(ColumnDefinition(c_column_name, type_name=pg_type))
        if with_row_id:
            pg_table.columns.append(ColumnDefinition('__rowid__', type_name='text'))
        pg_tables.append(pg_table)
        if ISDEBUG:
            logger.log("Table imported: {0}".format(c_table.name))
    session.shutdown()
    return pg_tables