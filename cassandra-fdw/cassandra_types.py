cql_none = 0
cql_bigint = 1
cql_blob = 2
cql_boolean = 3
cql_counter = 4
cql_date = 5
cql_decimal = 6
cql_double = 7
cql_float = 8
cql_inet = 9
cql_int = 10
cql_list = 11
cql_map = 12
cql_set = 13
cql_smallint = 14
cql_text = 15
cql_time = 16
cql_timestamp = 17
cql_timeuuid = 18
cql_tinyint = 19
cql_tuple = 20
cql_uuid = 21
cql_varint = 22
cql_ascii = 23

class CqlType:
    def __init__(self, main_type, sub_types):
        self.main_type = main_type
        self.sub_types = sub_types