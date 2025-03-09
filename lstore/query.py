from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import RID_COLUMN, BASE_RID_COLUMN, METADATA_COLUMNS, INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN
from datetime import datetime

MAX_VALUE=2 ** 64 - 1

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table: Table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        if primary_key not in self.table.page_directory or primary_key == MAX_VALUE:
            return False
        directory = self.table.page_directory[primary_key]
        self.table.update_value(RID_COLUMN, directory, MAX_VALUE)
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        self.table.insert_lock.acquire()
        rid = self.table.num_records + 92106429
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        metadata = [MAX_VALUE, rid, int(time), int(schema_encoding), rid]
        metadata.extend(list(columns))
        self.table.write_base_page(metadata)
        self.table.insert_lock.release()
        return True

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, search_key, search_key_index, projected_columns_index):
        rids = []
        if search_key_index == self.table.key:
            rids.append(self.table.key_rids[search_key])
        elif self.table.index.indices[search_key_index] is not None:
            rids.extend(self.table.index.locate(search_key_index, search_key))
        else:
            rids.extend(self.table.get_rids(search_key_index, search_key))
        records = []
        for rid in rids:
            record = self.table.get_record(rid)
            if rid == record[BASE_RID_COLUMN]:
                columns = record[METADATA_COLUMNS:METADATA_COLUMNS + self.table.num_columns + 1]
                if record[INDIRECTION_COLUMN] != MAX_VALUE:
                    record_tail = self.table.get_record(record[INDIRECTION_COLUMN])
                    columns = record_tail[METADATA_COLUMNS:METADATA_COLUMNS + self.table.num_columns + 1]
                    columns[self.table.key] = record[METADATA_COLUMNS + self.table.key]
                for column in range(self.table.num_columns):
                    if projected_columns_index[column] is None:
                        columns[column] = None
                records.append(Record(rid, search_key, columns))
        return records

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        rids = []
        records = []
        if search_key_index == self.table.key:
            rids.append(self.table.key_rids[search_key])
        elif self.table.index.indices[search_key_index] is not None:
            rids = self.table.index.locate(search_key_index, search_key)
            base_rids = []
            columns = []
            for rid in rids:
                record = self.table.get_record(rid)
                base_rid = record[BASE_RID_COLUMN]
                base_record = self.table.get_record(base_rid)
                tail_record = base_record
                relative_version = (relative_version * -1) + 1
                if base_record[INDIRECTION_COLUMN] != MAX_VALUE:
                    for i in range(relative_version):
                        tail_rid = tail_record[INDIRECTION_COLUMN]
                        tail_record = self.table.get_record(tail_rid)
                        columns = tail_record[METADATA_COLUMNS:METADATA_COLUMNS + self.table.num_columns + 1]
                        columns[self.table.key] = record[METADATA_COLUMNS + self.table.key]
                        if tail_rid == rid or tail_rid == base_rid:
                            break

                if tail_record == record:
                    record = Record(base_rid, search_key, columns)
                if base_rid not in base_rids:
                    records.append(record)
                    base_rids.append(base_rid)
            return records
        else:
            rids.extend(self.table.get_rids(search_key_index, search_key))
        for rid in rids:
            record = self.table.get_record(rid)
            columns = record[METADATA_COLUMNS:METADATA_COLUMNS + self.table.num_columns + 1]
            tail_record = record
            relative_version = (relative_version * -1) + 1
            if record[INDIRECTION_COLUMN] != MAX_VALUE:
                for i in range(relative_version):
                    tail_rid = tail_record[INDIRECTION_COLUMN]
                    tail_record = self.table.get_record(tail_rid)
                    columns = tail_record[METADATA_COLUMNS:METADATA_COLUMNS + self.table.num_columns + 1]
                    columns[self.table.key] = record[METADATA_COLUMNS + self.table.key]
                    if tail_rid == rid:
                        break
            records.append(Record(rid, search_key, columns))
        return records

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        if primary_key not in self.table.key_rids:
            return False
        rid = self.table.key_rids[primary_key]
        if rid not in self.table.page_directory or rid == MAX_VALUE:
            return False
        self.table.update_lock.acquire()
        record = self.table.get_record(rid)
        base_rid = record[RID_COLUMN]
        base_indirection = record[INDIRECTION_COLUMN]
        column_list = list(columns)
        new_tail_str = ''
        new_tail_rid = self.table.num_updates
        if base_indirection == MAX_VALUE:
            new_tail_indirection = rid
            for i in range(len(column_list)):
                if column_list[i] is None:
                    column_list[i] = record[i + METADATA_COLUMNS]
                    new_tail_str += '0'
                else:
                    new_tail_str += '1'
        else:
            tail_record = self.table.get_record(base_indirection)
            for i in range(len(column_list)):
                if column_list[i] is None:
                    column_list[i] = tail_record[i + METADATA_COLUMNS]
                    if (tail_record[SCHEMA_ENCODING_COLUMN] // (10 ** i)) % 10:
                        new_tail_str += '1'
                    else:
                        new_tail_str += '0'
                else:
                    new_tail_str += '1'
            new_tail_indirection = tail_record[RID_COLUMN]
        meta_data = [new_tail_indirection, new_tail_rid, int(datetime.now().strftime("%Y%m%d%H%M%S")), new_tail_str, base_rid]
        column_list[self.table.key] = MAX_VALUE
        meta_data.extend(column_list)
        self.table.write_tail_page(meta_data)
        base_encoding = record[SCHEMA_ENCODING_COLUMN]
        new_base_encoding = base_encoding or new_tail_str
        base_address = self.table.page_directory[rid]
        self.table.update_value(INDIRECTION_COLUMN, base_address, new_tail_rid)
        self.table.update_value(SCHEMA_ENCODING_COLUMN, base_address, new_base_encoding)
        self.table.update_lock.release()
        return True

    """0
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        summation = 0
        has_no_record = True
        for key in range(start_range, end_range + 1):
            if key in self.table.key_rids.keys():
                record = self.select(key, self.table.key, [1 for i in range(self.table.num_columns)])[0]
                value = record.columns[aggregate_column_index]
                if value is not None:
                    summation += value
                has_no_record = False
        if has_no_record:
            return False
        return summation

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        summation = 0
        has_no_record = True
        for key in range(start_range, end_range + 1):
            if key in self.table.key_rids.keys():
                record = self.select_version(key, self.table.key, [1 for i in range(self.table.num_columns)], relative_version)[0]
                value = record.columns[aggregate_column_index]
                if value is not None:
                    summation += value
                has_no_record = False
        if has_no_record:
            return False
        return summation

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
