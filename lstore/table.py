import os.path

from lstore.index import Index
from time import time
from lstore.util import eight_bytes_to_int, int_to_8_bytes
from lstore.config import RECORD_SIZE, METADATA_COLUMNS, PAGE_SIZE, RECORDS_PER_PAGE, BASE_PAGES_PER_RANGE, BASE_RID_COLUMN, RID_COLUMN
from lstore.page_range import PageRange
from lstore.lru import LRU
from threading import Lock


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key, database_directory):
        # The name of the table.
        self.name = name
        # The index of the table key in the columns.
        self.key = key
        #  The number of columns in the table, all columns are integers.
        self.num_columns = num_columns
        # A dictionary mapping record identifiers (RID) to their page addresses.
        self.page_directory = {}
        # An index object for the table.
        self.index = Index(self)
        # The directory where the database files are stored.
        self.database_directory = database_directory
        # The number of records in the table.
        self.num_records = 0
        # The number of record updates in the table.
        self.num_updates = 0
        # A dictionary storing page ranges in the buffer pool.
        self.buffer_pool: dict[int, PageRange] = {}
        # An LRU cache object for managing page ranges.
        self.lru = LRU(self)
        # A dictionary mapping keys to their corresponding record identifiers.
        self.key_rids = {}
        # A dictionary of locks for synchronization.
        self.locks = {}
        # A lock for insert operations.
        self.insert_lock: Lock = Lock()
        # A lock for update operations.
        self.update_lock: Lock = Lock()
        pass

    def open(self):
        """
        Open the table and load its metadata and page ranges from disk.
        """
        metadata_file = os.path.join(self.database_directory, self.name, 'metadata.table')
        if not os.path.exists(metadata_file) or not os.path.isfile(metadata_file):
            return
        with open(metadata_file, 'rb') as f:
            data = f.read()
        offset = 0
        # Read the number of page ranges
        num_page_ranges = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
        offset += RECORD_SIZE
        for _ in range(num_page_ranges):
            # Read the page range ID
            page_range_id = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            page_range_file = os.path.join(self.database_directory, self.name, f'{page_range_id}.page_range')
            page_range = self.read_page_range(page_range_file)
            self.buffer_pool[page_range_id] = page_range
            self.lru.page_range_created(page_range)
        # Read the number of key directories
        num_key_directories = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
        offset += RECORD_SIZE
        for _ in range(num_key_directories):
            key = eight_bytes_to_int(data[offset:offset+RECORD_SIZE])
            offset += RECORD_SIZE
            address = [None for _ in range(4)]
            address[0] = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            length = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            address[1] = data[offset:offset + length].decode('utf-8')
            offset += length
            address[2] = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            address[3] = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            self.page_directory[key] = address
        # Read the number of keys
        num_keys = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
        offset += RECORD_SIZE
        for _ in range(num_keys):
            key = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            rid = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
            offset += RECORD_SIZE
            self.key_rids[key] = rid

    def close(self):
        """
        Close the table and save its metadata and page ranges to disk.
        """
        if not os.path.exists(os.path.join(self.database_directory, self.name)):
            os.mkdir(os.path.join(self.database_directory, self.name))
        metadata_file = os.path.join(self.database_directory, self.name, 'metadata.table')
        data_size = 0
        data_size += 3 * RECORD_SIZE * len(self.buffer_pool) + RECORD_SIZE
        data_size += len(self.page_directory) * RECORD_SIZE * 5 + RECORD_SIZE
        data_size += len(self.key_rids) * RECORD_SIZE * 2 + RECORD_SIZE
        data = bytearray(data_size)
        offset = 0
        # Write the number of page ranges
        data[offset:offset + RECORD_SIZE] = int_to_8_bytes(len(self.buffer_pool))
        offset += RECORD_SIZE
        if len(self.buffer_pool) != 0:
            page_range = self.lru.oldest_page_range
            while page_range is not None:
                data[offset:offset + RECORD_SIZE] = int_to_8_bytes(page_range.index)
                offset += RECORD_SIZE
                self.lru.delete_page()
                page_range = self.lru.oldest_page_range
        data[offset:offset + RECORD_SIZE] = int_to_8_bytes(len(self.page_directory))
        offset += RECORD_SIZE
        for key, address in self.page_directory.items():
            data[offset:offset + RECORD_SIZE] = int_to_8_bytes(key)
            offset += RECORD_SIZE
            data[offset: offset + RECORD_SIZE] = int_to_8_bytes(address[0])
            offset += RECORD_SIZE
            b = address[1].encode('utf-8')
            data[offset:offset + RECORD_SIZE] = int_to_8_bytes(len(b))
            offset += RECORD_SIZE
            data[offset: offset + len(b)] = b[:]
            offset += len(b)
            data[offset: offset + RECORD_SIZE] = int_to_8_bytes(address[2])
            offset += RECORD_SIZE
            data[offset: offset + RECORD_SIZE] = int_to_8_bytes(address[3])
            offset += RECORD_SIZE
        data[offset: offset + RECORD_SIZE] = int_to_8_bytes(len(self.key_rids))
        offset += RECORD_SIZE
        for key, rid in self.key_rids.items():
            data[offset: offset + RECORD_SIZE] = int_to_8_bytes(key)
            offset += RECORD_SIZE
            data[offset: offset + RECORD_SIZE] = int_to_8_bytes(rid)
            offset += RECORD_SIZE
        with open(metadata_file, 'wb') as f:
            f.write(data)

    def read_page_range(self, page_range_file):
        """
        Read a page range from disk.

        Args:
            page_range_file (str): The path to the page range file.

        Returns:
            PageRange: The loaded page range object.
        """
        page_range = PageRange(self.num_columns + METADATA_COLUMNS)
        with open(page_range_file, 'rb') as f:
            data = f.read()
        offset = 0
        num_base_pages = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
        offset += RECORD_SIZE
        num_tail_pages = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
        offset += RECORD_SIZE
        for i in range(num_base_pages):
            page_range.create_page()
            for column in range(page_range.columns):
                page = page_range.base_pages[-1][column]
                page.num_records = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
                offset += RECORD_SIZE
                page.data = bytearray(data[offset:offset + PAGE_SIZE])
                offset += PAGE_SIZE
                page.is_dirty = False
        for i in range(num_tail_pages):
            page_range.create_page(True)
            for column in range(page_range.columns):
                page = page_range.tail_pages[-1][column]
                page.num_records = eight_bytes_to_int(data[offset:offset + RECORD_SIZE])
                offset += RECORD_SIZE
                page.data = bytearray(data[offset:offset + PAGE_SIZE])
                offset += PAGE_SIZE
                # page.is_dirty = False
        page_range.is_dirty = False
        return page_range

    def write_page_range(self, page_range_file, page_range: PageRange):
        """
        Write a page range to disk.

        Args:
            page_range_file (str): The path to the page range file.
            page_range (PageRange): The page range object to be written.
        """
        num_base_pages = len(page_range.base_pages)
        num_tail_pages = len(page_range.tail_pages)
        data = bytearray(((num_base_pages + num_tail_pages) * (PAGE_SIZE + RECORD_SIZE) * (self.num_columns + METADATA_COLUMNS)) + 2 * RECORD_SIZE)
        offset = 0
        data[offset:offset + RECORD_SIZE] = int_to_8_bytes(num_base_pages)
        offset += RECORD_SIZE
        data[offset:offset + RECORD_SIZE] = int_to_8_bytes(num_tail_pages)
        offset += RECORD_SIZE
        for row in range(num_base_pages):
            for col in range(self.num_columns + METADATA_COLUMNS):
                page = page_range.base_pages[row][col]
                data[offset:offset + RECORD_SIZE] = int_to_8_bytes(page.num_records)
                offset += RECORD_SIZE
                data[offset:offset + PAGE_SIZE] = page.data
                offset += PAGE_SIZE
        for row in range(num_tail_pages):
            for col in range(self.num_columns + METADATA_COLUMNS):
                page = page_range.tail_pages[row][col]
                data[offset:offset + RECORD_SIZE] = int_to_8_bytes(page.num_records)
                offset += RECORD_SIZE
                data[offset:offset + PAGE_SIZE] = page.data
                offset += PAGE_SIZE
        with open(page_range_file, 'wb') as f:
            f.write(data)

    def calculate_page_position(self, rid: int = -1):
        """
        Calculate the page range index and page index for a given record identifier.

        Args:
            rid (int, optional): The record identifier. Defaults to -1.

        Returns:
            tuple: A tuple containing the page range index and page index.
        """
        if rid == -1:
            page_range_idx = self.num_records // (RECORDS_PER_PAGE * BASE_PAGES_PER_RANGE)
            page_idx = self.num_records // RECORDS_PER_PAGE
        else:
            page_range_idx = (rid - 92106429) // (RECORDS_PER_PAGE * BASE_PAGES_PER_RANGE)
            page_idx = -1
        page_idx = page_idx % BASE_PAGES_PER_RANGE
        return page_range_idx, page_idx

    def write_base_page(self, columns):
        """
        Write a record to the base pages.

        Args:
            columns (list): A list of column values for the record.
        """
        page_range_idx, page_idx = self.calculate_page_position()
        for i, column in enumerate(columns):
            page = self.get_page(page_range_idx, i)
            page_range = self.buffer_pool[page_range_idx]
            page_range.is_dirty = True
            page.write(column)
            # print(int(page_idx))
            page_range.base_pages[int(page_idx)][i] = page
            self.buffer_pool[page_range_idx] = page_range
        rid = columns[RID_COLUMN]
        self.page_directory[rid] = [page.num_records - 1, 'base', page_range_idx, page_idx]
        self.num_records += 1
        self.key_rids[columns[self.key + METADATA_COLUMNS]] = rid
        self.index.push_index(columns[METADATA_COLUMNS:len(columns) + 1], rid)

    def write_tail_page(self, columns):
        """
        Write a record to the tail pages.

        Args:
            columns (list): A list of column values for the record.
        """
        page_range_idx, page_idx = self.calculate_page_position(columns[BASE_RID_COLUMN])
        for i, column in enumerate(columns):
            page_range = self.buffer_pool[page_range_idx]
            page_range.is_dirty = True
            page = self.get_page(page_range_idx, i, False)
            page.write(column)
            page_idx = len(page_range.tail_pages) - 1
            page_range.tail_pages[-1][i] = page
            self.buffer_pool[page_range_idx] = page_range
        rid = columns[RID_COLUMN]
        self.page_directory[rid] = [page.num_records - 1, 'tail', page_range_idx, page_idx]
        self.num_updates += 1
        self.index.push_index(columns[METADATA_COLUMNS:len(columns) + 1], rid)

    def get_rids(self, column, value):
        """
        Get the record identifiers (RIDs) of records that match the given column value.

        Args:
            column (int): The column index.
            value (int): The column value to match.

        Returns:
            list: A list of record identifiers.
        """
        rids = []
        for rid in self.page_directory:
            record = self.get_record(rid)
            if record[column + METADATA_COLUMNS] == value:
                rids.append(rid)
        return rids

    def get_record(self, rid):
        """
        Get a record by its record identifier (RID).

        Args:
            rid (int): The record identifier.

        Returns:
            list: A list of column values for the record.
        """
        record = []
        directory = self.page_directory[rid]
        for column in range(METADATA_COLUMNS + self.num_columns):
            record.append(self.get_value(column, directory))
        return record

    def get_value(self, column, directory):
        """
        Get the value of a specific column in a record.

        Args:
            column (int): The column index.
            directory (list): The page directory entry for the record.

        Returns:
            int: The value of the column.
        """
        page = self.get_page(directory[2], column, directory[1] == 'base', False, directory[3])
        return page.read(directory[0])

    def update_value(self, column, directory, value):
        """
        Update the value of a specific column in a record.

        Args:
            column (int): The column index.
            directory (list): The page directory entry for the record.
            value (int): The new value for the column.
        """
        page = self.get_page(directory[2], column, directory[1] == 'base', False, directory[3])
        page.write(value, directory[0])

    def get_page(self, page_range_index, column, is_base=True, current=True, page_number=0):
        """
        Get a page from the buffer pool or disk.

        Args:
            page_range_index (int): The index of the page range.
            column (int): The column index.
            is_base (bool, optional): Whether the page is a base page. Defaults to True.
            current (bool, optional): Whether to get the current page. Defaults to True.
            page_number (int, optional): The page number within the page range. Defaults to 0.

        Returns:
            Page: The requested page object.
        """
        page_range = None
        if page_range_index in self.buffer_pool:
            page_range = self.buffer_pool[page_range_index]
            page_range.index = page_range_index
            self.lru.page_range_accessed(page_range)
        elif os.path.exists(os.path.join(self.database_directory, self.name, f'{page_range_index}.page_range')) and os.path.isfile(os.path.join(self.database_directory, self.name, f'{page_range_index}.page_range')):
            page_range = self.read_page_range(os.path.join(self.database_directory, self.name, f'{page_range_index}.page_range'))
            self.buffer_pool[page_range_index] = page_range
            self.lru.page_range_created(page_range)
        else:
            # print(self.num_columns,METADATA_COLUMNS)
            page_range = PageRange(self.num_columns + METADATA_COLUMNS)
            page_range.index = page_range_index
            self.buffer_pool[page_range_index] = page_range
            self.lru.page_range_created(page_range)
        if current:
            if is_base:
                return page_range.get_base_page_by_column(column)
            else:
                return page_range.get_tail_page_by_column(column)
        else:
            if is_base:
                return page_range.base_pages[page_number][column]
            else:
                return page_range.tail_pages[page_number][column]
