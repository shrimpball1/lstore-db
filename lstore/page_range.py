from lstore.config import BASE_PAGES_PER_RANGE
from lstore.page import Page


class PageRange:
    def __init__(self, columns: int):
        """
        Initialize a new PageRange object.

        Args:
            columns (int): The number of columns in the page range.
        """
        # The number of columns in the page range
        self.columns = columns
        # A 2D list storing base pages, where each inner list represents a set of pages for all columns
        self.base_pages: list[list[Page]] = []
        # A 2D list storing tail pages, where each inner list represents a set of pages for all columns
        self.tail_pages: list[list[Page]] = []
        # A flag indicating whether the page range has been modified
        self.is_dirty = True
        # The index of the page range
        self.index = None
        # A reference to the next page range in the linked list
        self.next_page_range = None
        # A reference to the previous page range in the linked list
        self.previous_page_range = None

    def create_page(self, is_tail=False):
        """
        Create a new set of pages for all columns.

        Args:
            is_tail (bool, optional): Whether to create tail pages. Defaults to False.
        """
        pages = []
        for _ in range(self.columns):
            pages.append(Page())
        if not is_tail:
            self.base_pages.append(pages)
        else:
            self.tail_pages.append(pages)
        self.is_dirty = True

    def get_base_page_by_column(self, column: int):
        """
        Get the appropriate base page for the given column.
        If there are no base pages or the last base page for the column is full, create a new set of base pages.

        Args:
            column (int): The column index.

        Returns:
            Page: The base page for the specified column.
        """
        if len(self.base_pages) == 0:
            self.create_page()
        if not self.base_pages[-1][column].has_capacity():
            self.create_page()
        return self.base_pages[-1][column]

    def get_tail_page_by_column(self, column: int):
        """
        Get the appropriate tail page for the given column.
        If there are no tail pages or the last tail page for the column is full, create a new set of tail pages.

        Args:
            column (int): The column index.

        Returns:
            Page: The tail page for the specified column.
        """
        if len(self.tail_pages) == 0:
            self.create_page(True)
        if not self.tail_pages[-1][column].has_capacity():
            self.create_page(True)
        return self.tail_pages[-1][column]



