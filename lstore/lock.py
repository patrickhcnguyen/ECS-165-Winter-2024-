import threading

class LockManager:
    def __init__(self):
        self.locks = {} # key: rid, value: lock

    """
    parameters:
    rid_list - list of rid's to acquire read locks
    """
    def acquire_read_locks(self, rid_list):
        for rid in rid_list:
            if rid in self.locks:
                pass
            else:
                self.locks[rid] = Lock()
            self.locks[rid].get_shared_lock()

    def acquire_exclusive_lock(self, rid):
        if rid in self.locks:
            pass
        else:
            self.locks[rid] = Lock()
        self.locks[rid].get_exclusive_lock()

    def release_all_locks(self, held_locks):
        print("release all locks")
        pass


class Lock:
    def __init__(self):
        self.read_count = 0
        self.write_count = 0

    # read-lock: other transactions can only read but not write
    def get_shared_lock(self): #read lock
        if self.write_count == 0:
            self.read_count += 1
            return True
        return False

    # write-lock: other transactions cannot read or write
    def get_exclusive_lock(self):
        if self.read_count == 0 and self.write_count == 0:
            self.write_count += 1
            return True
        return False

    def release_shared_lock(self):
        if self.read_count > 0:
            self.read_count -= 1
            return True
        return False

    def release_exclusive_lock(self):
        if self.write_count > 0:
            self.write_count -= 1
            return True
        return False