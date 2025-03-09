import threading
from readerwriterlock import rwlock
class LockManager:
    def __init__(self):
        # Create a fair read - write lock object.
        # A fair lock ensures that threads are served in the order they request the lock.
        self.lock = rwlock.RWLockFair()
        # Generate a read lock from the fair read - write lock.
        self.reader_lock = self.lock.gen_rlock()
        # Generate a write lock from the fair read - write lock.
        self.writer_lock = self.lock.gen_wlock()

    def release_reader_lock(self):
        # Try to release the read lock.
        # If the lock is not held by the current thread, a RuntimeError will be raised.
        # In such a case, simply ignore the error.
        try:
            self.reader_lock.release()
        except RuntimeError:
            pass

    def release_writer_lock(self):
        # Try to release the write lock.
        # If the lock is not held by the current thread, a RuntimeError will be raised.
        # In such a case, simply ignore the error.
        try:
            self.writer_lock.release()
        except RuntimeError:
            pass

    def acquire_reader_lock(self):
        # Try to acquire the read lock in a non - blocking way.
        # If the lock can be acquired immediately, return True; otherwise, return False.
        return self.reader_lock.acquire(blocking=False)

    def acquire_writer_lock(self):
        # Try to acquire the write lock in a non - blocking way.
        # If the lock can be acquired immediately, return True; otherwise, return False.
        return self.writer_lock.acquire(blocking=False)
