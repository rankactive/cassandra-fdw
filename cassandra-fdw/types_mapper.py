import time_utils
from datetime import datetime
import uuid
import json
from decimal import Decimal

def map_object_to_type(obj, validator):
    if obj is None:
        return None
    obj_type = type(obj)
    if validator.startswith('frozen<tuple<'):
        types = validator[13:-2].split(',')
        tuplearray = json.loads(obj)
        tuple_idx = 0
        lst = []
        for t in types:
            tuple_object = map_object_to_type(tuplearray[tuple_idx], t.strip())
            lst.append(tuple_object)
            tuple_idx += 1
        return tuple(lst)
    elif validator.startswith('set<'):
        set_type = validator[4:-1]
        output_set = frozenset(map(lambda t: map_object_to_type(t, set_type), obj))
        return output_set
    elif validator.startswith('map<'):
        map_types = validator[4:-1].split(',')
        map_obj = json.loads(obj)
        output_dict = {}
        for k in map_obj:
            key_obj = map_object_to_type(k, map_types[0].strip())
            value_obj = map_object_to_type(map_obj[k], map_types[1].strip())
            output_dict[key_obj] = value_obj
        return output_dict
    elif validator.startswith('list<'):
        list_type = validator[5:-1]
        list = map(lambda t: map_object_to_type(t, list_type), obj)
        return list
    if obj_type is str:
        str_obj = obj.encode('utf8')
    elif obj_type is datetime:
        return obj
    else:
        str_obj = unicode(obj)
    return {
        'uuid': lambda: uuid.UUID(str_obj),
        'bigint': lambda: long(str_obj),
        'boolean': lambda: bool(str_obj),
        'decimal': lambda: Decimal(str_obj),
        'double': lambda: float(str_obj),
        'float': lambda: float(str_obj),
        'int': lambda: int(str_obj),
        'timestamp': lambda: time_utils.parse_date_string(str_obj),
        'timeuuid': lambda: uuid.UUID(str_obj),
        'text': lambda: str_obj,
        'inet': lambda: str_obj,
        'counter': lambda: long(str_obj),
        'varchar': lambda: str_obj,
        'varint': lambda: int(str_obj),
        'blob': lambda: str_obj,
        'ascii': lambda: str_obj,
        'tinyint': lambda: int(str_obj),
        'smallint': lambda: int(str_obj),
        'time': lambda: time_utils.parse_time_string(str_obj),
        'date': lambda: datetime.strptime(str_obj, '%Y-%m-%d')
    }[validator]()

def get_pg_type(cassandra_type):
    dict = {
        'ascii': lambda: 'bytea',
        'blob': lambda: 'bytea',
        'double': lambda: 'float8',
        'float': lambda: 'float4',
        'time': lambda: 'timetz',
        'timestamp': lambda: 'timestamptz',
        'timeuuid': lambda: 'uuid',
        'tinyint': lambda: 'smallint',
        'varchar': lambda: 'text',
        'varint': lambda: 'int',
        'counter': lambda: 'bigint'
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
    return dict[cassandra_type]()