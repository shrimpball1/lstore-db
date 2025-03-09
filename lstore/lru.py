import os

from lstore.page_range import PageRange


class LRU:
    def __init__(self, table):
        """
        Initialize the LRU (Least Recently Used) cache for page ranges.

        Args:
            table (table.Table): The table object associated with this LRU cache.
        """
        # Reference to the table object that this LRU cache belongs to
        self.table: 'table.Table' = table
        # The most recently accessed page range, initially set to None
        self.latest_page_range: PageRange | None = None
        # The least recently accessed page range, initially set to None
        self.oldest_page_range: PageRange | None = None
        # The current number of pages in the cache
        self.num_pages = 0
        # The maximum number of pages the cache can hold
        self.max_pages = 128

    def page_range_created(self, page_range: PageRange):
        """
        Handle the creation of a new page range and add it to the LRU cache.

        Args:
            page_range (PageRange): The newly created page range.
        """
        if self.oldest_page_range is None:
            # If the cache is empty, set the newly created page range as the oldest one
            self.oldest_page_range = page_range
        if self.latest_page_range is not None:
            # Link the new page range to the existing latest page range
            page_range.previous_page_range = self.latest_page_range
            self.latest_page_range.next_page_range = page_range
        # Update the latest page range to the newly created one
        self.latest_page_range = page_range
        # Increase the number of pages in the cache
        self.num_pages += 16
        if self.num_pages >= self.max_pages:
            # If the cache is full, delete the least recently used page range
            self.delete_page()

    def page_range_accessed(self, page_range: PageRange):
        """
        Handle the access of an existing page range and move it to the front of the LRU cache.

        Args:
            page_range (PageRange): The accessed page range.
        """
        if self.latest_page_range != page_range and self.oldest_page_range != page_range:
            # If the accessed page range is neither the latest nor the oldest
            # Remove the page range from its current position in the linked list
            page_range.next_page_range.previous_page_range = page_range.previous_page_range
            page_range.previous_page_range.next_page_range = page_range.next_page_range
            # Move the page range to the front of the linked list
            page_range.previous_page_range = self.latest_page_range
            self.latest_page_range.next_page_range = page_range
            # Update the latest page range to the accessed one
            self.latest_page_range = page_range

    def delete_page(self):
        """
        Delete the least recently used page range from the LRU cache.
        If the page range is dirty, write it back to disk before deletion.
        """
        # Update the number of pages in the cache
        self.num_pages = self.num_pages - 16 - len(self.oldest_page_range.tail_pages)
        if self.oldest_page_range.next_page_range is not None:
            # If there is a next page range, unlink the oldest page range
            self.oldest_page_range.next_page_range.previous_page_range = None
            # Store the oldest page range to be deleted
            oldest_page_range = self.oldest_page_range
            # Update the oldest page range to the next one
            self.oldest_page_range = self.oldest_page_range.next_page_range
        else:
            # If there is no next page range, the cache will be empty after deletion
            oldest_page_range = self.oldest_page_range
            self.oldest_page_range = None
        if oldest_page_range.is_dirty:
            # If the page range is dirty, write it back to disk
            self.table.write_page_range(os.path.join(self.table.database_directory, self.table.name, f'{oldest_page_range.index}.page_range'), oldest_page_range)
        # Delete the least recently used page range
        del oldest_page_range