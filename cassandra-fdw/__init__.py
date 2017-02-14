from multicorn import ForeignDataWrapper
from cassandra_provider import CassandraProvider
from properties import ISDEBUG
import properties
import schema_importer
import time

class CassandraFDW(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(CassandraFDW, self).__init__(options, columns)
        self.init_options = options
        self.init_columns = columns
        self.cassandra_provider = None
        self.concurency_level = int(options.get('modify_concurency', properties.DEFAULT_CONCURENCY_LEVEL))
        self.per_transaction_connection = options.get('per_transaction_connection', properties.PER_TRANSACTION_CONNECTION) == 'True'
        self.modify_items = []

    def build_cassandra_provider(self):
        if self.cassandra_provider == None:
            self.cassandra_provider = CassandraProvider(self.init_options, self.init_columns)

    @classmethod
    def import_schema(self, schema, srv_options, options, restriction_type, restricts):
        return schema_importer.import_schema(schema, srv_options, options, restriction_type, restricts)

    def insert(self, new_values):
        if self.concurency_level > 1:
            self.modify_items.append(('insert', new_values))
            if len(self.modify_items) >= properties.BATCH_MODIFY_THRESHOLD:
                self.end_modify()
            return new_values
        else:
            return self.cassandra_provider.insert(new_values)

    def delete(self, rowid):
        if self.concurency_level > 1:
            self.modify_items.append(('delete', rowid))
            if len(self.modify_items) >= properties.BATCH_MODIFY_THRESHOLD:
                self.end_modify()
            return { }
        else:
            return self.cassandra_provider.delete(rowid)

    def update(self, rowid, new_values):
        if ISDEBUG:
            logger.log(u"requested update {0}".format(new_values))
        self.insert(new_values)
        return new_values

    def execute(self, quals, columns, sort_keys=None):
        self.scan_start_time = time.time()
        return self.cassandra_provider.execute(quals, columns, sort_keys)

    def can_sort(self, sort_keys):
        return []

    def begin(self, serializable):
        self.build_cassandra_provider()
        if ISDEBUG:
            logger.log("begin: {0}".format(serializable))

    def commit(self):
        if ISDEBUG:
            logger.log("commit")
        if self.per_transaction_connection:
            self.close_cass_connection()
        pass

    def close_cass_connection(self):
        if self.cassandra_provider != None:
            self.cassandra_provider.close()
            self.cassandra_provider = None

    def end_modify(self):
        try:
            mod_len = len(self.modify_items)
            if mod_len > 0:
                if ISDEBUG:
                    logger.log("end modify")
                    logger.log("modify concurrency level: {0}".format(self.concurency_level))
                self.cassandra_provider.execute_modify_items(self.modify_items, self.concurency_level)
        finally:
            self.modify_items = []
            pass

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        return self.cassandra_provider.build_select_stmt(quals, columns, self.cassandra_provider.allow_filtering, verbose)

    def end_scan(self):
        if ISDEBUG:
            logger.log("end_scan. Total time: {0} ms".format((time.time() - self.scan_start_time) * 1000))
        pass

    def pre_commit(self):
        if ISDEBUG:
            logger.log("pre commit")
        pass

    def rollback(self):
        if ISDEBUG:
            logger.log("rollback")
        pass

    def sub_begin(self, level):
        if ISDEBUG:
            logger.log("sub begin {0}".format(level))
        pass

    def sub_commit(self, level):
        if ISDEBUG:
            logger.log("sub commit {0}".format(level))
        pass

    def sub_rollback(self, level):
        if ISDEBUG:
            logger.log("sub rollback {0}".format(level))
        pass

    @property
    def rowid_column(self):
        return self.cassandra_provider.get_row_id_column()

    def get_rel_size(self, quals, columns):
        return self.cassandra_provider.get_rel_size(quals, columns)

    def get_path_keys(self):
        self.scan_start_time = time.time()
        return self.cassandra_provider.get_path_keys()