from lstore.table import Table, Record
from lstore.index import Index
from lstore.lockmanager import LockManager
from lstore.query import Query


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.table = None
        # A set to keep track of the keys for which read locks are acquired.
        self.read_locks = set()
        # A set to keep track of the keys for which write locks are acquired.
        self.write_locks = set()
        # A set to keep track of the keys for which insert locks are acquired.
        self.insert_locks = set()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.table is None:
            self.table = table

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        """
        Execute the transaction.
        This method attempts to acquire write locks for all the keys involved in the queries.
        If it fails to acquire a write lock for any key, it aborts the transaction.
        If all write locks are acquired successfully, it proceeds to commit the transaction.
        Returns:
            bool: True if the transaction commits successfully, False if it aborts.
        """
        for query, args in self.queries:
            key = args[0]
            table_locks = self.table.locks
            if key not in table_locks:
                table_locks[key] = LockManager()
                self.insert_locks.add(key)
            if key not in self.write_locks.union(self.insert_locks):
                if table_locks[key].acquire_writer_lock():
                    self.write_locks.add(key)
                else:
                    return self.abort()
        return self.commit()

    def abort(self):
        """
        Abort the transaction.
        This method releases all the acquired read locks, write locks, and deletes the lock managers
        associated with the keys for which insert locks were acquired.
        Returns:
            bool: False to indicate that the transaction has been aborted.
        """
        table_locks = self.table.locks
        for key in self.read_locks:
            table_locks[key].release_reader_lock()
        for key in self.write_locks:
            table_locks[key].release_writer_lock()
        for key in self.insert_locks:
            del table_locks[key]
        return False

    def commit(self):
        """
        Commit the transaction.
        This method executes all the queries in the transaction, releases all the acquired read locks,
        write locks, and release the writer locks for the keys in the insert_locks set.
        Returns:
            bool: True to indicate that the transaction has been committed successfully.
        """
        table_locks = self.table.locks
        for query, args in self.queries:
            query(*args)
            if query == Query.delete:
                key = args[0]
                del table_locks[key]
                if key in self.write_locks:
                    self.write_locks.remove(key)
                if key in self.insert_locks:
                    self.insert_locks.remove(key)
        for key in self.read_locks:
            table_locks[key].release_reader_lock()
        for key in self.write_locks:
            table_locks[key].release_writer_lock()
        for key in self.insert_locks:
            table_locks[key].release_writer_lock()
        return True
