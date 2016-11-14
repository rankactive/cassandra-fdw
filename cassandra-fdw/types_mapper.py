import time_utils
from datetime import datetime
import uuid
import json
import cassandra_types
from cassandra_types import CqlType
from decimal import Decimal

def get_cql_type_from_validator(validator):
    if validator.startswith('frozen<tuple<'):
        types = validator[13:-2].split(',')
        sub_types = []
        for t in types:
            sub_types.append(get_cql_type_from_validator(t.strip()))
        return CqlType(cassandra_types.cql_tuple, sub_types)
    elif validator.startswith('set<'):
        set_type = validator[4:-1].strip()
        sub_type = get_cql_type_from_validator(set_type)
        return CqlType(cassandra_types.cql_set, [sub_type])
    elif validator.startswith('map<'):
        map_types = validator[4:-1].split(',')
        key_type = get_cql_type_from_validator(map_types[0].strip())
        value_type = get_cql_type_from_validator(map_types[1].strip())
        return CqlType(cassandra_types.cql_map, [key_type, value_type])
    elif validator.startswith('list<'):
        list_type = validator[5:-1]
        sub_type = get_cql_type_from_validator(list_type)
        return CqlType(cassandra_types.cql_list, [sub_type])
    simple_type = {
        'uuid': cassandra_types.cql_uuid,
        'bigint': cassandra_types.cql_bigint,
        'boolean': cassandra_types.cql_boolean,
        'decimal': cassandra_types.cql_decimal,
        'double': cassandra_types.cql_double,
        'float': cassandra_types.cql_float,
        'int': cassandra_types.cql_int,
        'timestamp': cassandra_types.cql_timestamp,
        'timeuuid': cassandra_types.cql_timeuuid,
        'text': cassandra_types.cql_text,
        'inet': cassandra_types.cql_inet,
        'counter': cassandra_types.cql_counter,
        'varint': cassandra_types.cql_varint,
        'blob': cassandra_types.cql_blob,
        'ascii': cassandra_types.cql_ascii,
        'tinyint': cassandra_types.cql_tinyint,
        'smallint': cassandra_types.cql_smallint,
        'time': cassandra_types.cql_time,
        'date': cassandra_types.cql_date
    }[validator]
    return CqlType(simple_type, [])

def map_object_to_type(obj, cql_type):
    if obj is None:
        return None
    if cql_type.main_type == cassandra_types.cql_tuple:
        tuplearray = json.loads(obj)
        tuple_idx = 0
        lst = []
        for t in cql_type.sub_types:
            tuple_object = map_object_to_type(tuplearray[tuple_idx], t)
            lst.append(tuple_object)
            tuple_idx += 1
        return tuple(lst)
    elif cql_type.main_type == cassandra_types.cql_set:
        output_set = frozenset(map(lambda t: map_object_to_type(t, cql_type.sub_types[0]), obj))
        return output_set
    elif cql_type.main_type == cassandra_types.cql_map:
        map_obj = json.loads(obj)
        output_dict = {}
        for k in map_obj:
            key_obj = map_object_to_type(k, cql_type.sub_types[0].strip())
            value_obj = map_object_to_type(map_obj[k], cql_type.sub_types[1].strip())
            output_dict[key_obj] = value_obj
        return output_dict
    elif cql_type.main_type == cassandra_types.cql_list:
        list = map(lambda t: map_object_to_type(t, cql_type.sub_types[0]), obj)
        return list
    if obj is datetime:
        return obj
    return {
        cassandra_types.cql_uuid: lambda: obj if obj is uuid.UUID else uuid.UUID(obj),
        cassandra_types.cql_bigint: lambda: obj if obj is long else long(str(obj)),
        cassandra_types.cql_boolean: lambda: obj if obj is bool else bool(str(obj)),
        cassandra_types.cql_decimal: lambda: obj if obj is Decimal else Decimal(str(obj)),
        cassandra_types.cql_double: lambda: obj if obj is float else float(str(obj)),
        cassandra_types.cql_float: lambda: obj if obj is float else float(str(obj)),
        cassandra_types.cql_int: lambda: obj if obj is int else int(str(obj)),
        cassandra_types.cql_timestamp: lambda: time_utils.parse_date_string(str(obj)),
        cassandra_types.cql_timeuuid: lambda: obj if obj is uuid.UUID else uuid.UUID(str(obj)),
        cassandra_types.cql_text: lambda: obj if obj is unicode else obj.encode('utf8'),
        cassandra_types.cql_inet: lambda: str(obj),
        cassandra_types.cql_counter: lambda: obj if obj is long else long(str(obj)),
        cassandra_types.cql_varint: lambda: obj if obj is int else int(str(obj)),
        cassandra_types.cql_blob: lambda: unicode(obj),
        cassandra_types.cql_ascii: lambda: unicode(obj),
        cassandra_types.cql_tinyint: lambda: obj if obj is int else int(str(obj)),
        cassandra_types.cql_smallint: lambda: obj if obj is int else int(str(obj)),
        cassandra_types.cql_time: lambda: time_utils.parse_time_string(str(obj)),
        cassandra_types.cql_date: lambda: datetime.strptime(str(obj), '%Y-%m-%d')
    }[cql_type.main_type]()

def get_pg_type(cassandra_type):
    dict = {
        'ascii': 'bytea',
        'blob': 'bytea',
        'double': 'float8',
        'float': 'float4',
        'time': 'timetz',
        'timestamp': 'timestamptz',
        'timeuuid': 'uuid',
        'tinyint': 'smallint',
        'varchar': 'text',
        'varint': 'int',
        'counter': 'bigint'
    }
    if cassandra_type.startswith('frozen<tuple<') or cassandra_type.startswith('map<'):
        return 'json'
    elif cassandra_type.startswith('set<'):
        set_type = cassandra_type[4:-1]
        return set_type + '[]'
    elif cassandra_type.startswith('list<'):
        list_type = cassandra_type[5:-1]
        return list_type + '[]'
    if cassandra_type not in dict:
        return cassandra_type
    return dict[cassandra_type]