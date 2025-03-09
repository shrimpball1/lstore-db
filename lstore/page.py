from lstore.config import RECORDS_PER_PAGE
from lstore.util import int_to_8_bytes, eight_bytes_to_int


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.is_dirty = True

    def has_capacity(self):
        """
        Check if the page has enough space to store a new record.
        Returns:
            bool: True if the number of records is less than the maximum number of records per page, False otherwise.
        """
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value: int, index: int | None = None):
        """
        Write an integer value to the page.
        Args:
            value (int): The integer value to be written.
            index (int | None, optional): The index where the value should be written. If None, the value will be appended to the end.
        Raises:
            Exception: If the page is full and index is None.
        """
        if index is None:
            if not self.has_capacity():
                raise Exception()
            index = self.num_records
            self.data[index * 8:index * 8 + 8] = int_to_8_bytes(value)
            self.num_records += 1
        else:
            self.data[index * 8:index * 8 + 8] = int_to_8_bytes(value)
        self.is_dirty = True

    def read(self, index):
        """
        Read an integer value from the specified index in the page.
        Args:
            index (int): The index from which to read the value.
        Returns:
            int: The integer value read from the page.
        """
        return eight_bytes_to_int(self.data[index * 8:index * 8 + 8])
