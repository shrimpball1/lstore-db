import os

from lstore.table import Table
from lstore.util import eight_bytes_to_int, int_to_8_bytes
from lstore.config import RECORD_SIZE


class Database:

    def __init__(self):
        self.tables: list[Table] = []
        self.database_directory = ''

    # Not required for milestone1
    def open(self, path):
        self.database_directory = path
        metadata_file = os.path.join(self.database_directory, 'metadata.db')
        if not os.path.exists(metadata_file) or not os.path.isfile(metadata_file):
            return
        with open(metadata_file, 'rb') as f:
            data = f.read()
        offset = 0
        tables_number = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
        offset += RECORD_SIZE
        for _ in range(tables_number):
            table_name_length = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            table_name = data[offset:offset + table_name_length].decode('utf-8')
            offset += table_name_length
            table_num_columns = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            table_key_index = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            table = self.create_table(table_name, table_num_columns, table_key_index)
            table.open()

    def close(self):
        try:
            os.mkdir(self.database_directory)
        except FileExistsError:
            pass
        metadata_file = os.path.join(self.database_directory, 'metadata.db')
        data = bytearray(RECORD_SIZE + (((2 * RECORD_SIZE) + RECORD_SIZE * 8) * len(self.tables)))
        offset = 0
        data[offset:offset + RECORD_SIZE] = int_to_8_bytes(len(self.tables))
        offset += RECORD_SIZE
        for table in self.tables:
            table_name = table.name.encode('utf-8')
            table_name_length = len(table_name)
            data[offset:offset + RECORD_SIZE] = int_to_8_bytes(table_name_length)
            offset += RECORD_SIZE
            data[offset:offset + table_name_length] = table_name
            offset += table_name_length
            data[offset:offset + RECORD_SIZE] = int_to_8_bytes(table.num_columns)
            offset += RECORD_SIZE
            data[offset:offset + RECORD_SIZE] = int_to_8_bytes(table.key)
            offset += RECORD_SIZE
        for table in self.tables:
            table.close()
        with open(metadata_file, 'wb') as f:
            f.write(data)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index, self.database_directory)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        self.tables = [table for table in self.tables if table.name != name]

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        return None
