import threading

class LockManager:
    def __init__(self):
        self.locks = {} # key: rid, value: lock
        self.thread_lock = threading.Lock() #this variable captures whichever thread accesses this LockManager object first (removing race condition to acquire the locks)

    """
    parameters:
    rid_list - list of rid's to acquire read locks
    """
    def acquire_read_locks(self, rid_list):
        with self.thread_lock:
            for rid in rid_list:
                if rid not in self.locks:
                    self.locks[rid] = Lock()
                success = self.locks[rid].get_shared_lock()
                if not success:
                    return False
                print("rid: ", rid, "read_count", self.locks[rid].read_count)
            return True

    def acquire_exclusive_lock(self, rid):
        with self.thread_lock:
            if rid in self.locks:
                pass
            else:
                self.locks[rid] = Lock()
            return self.locks[rid].get_exclusive_lock()

    def release_all_locks(self, held_locks):
        print("release locks")
        for rid, locks in held_locks.items():
            # print("rid: ", rid, " locks: ", locks)
            for lock in locks:
                if lock == 'r':
                    self.locks[rid].release_shared_lock()
                else: #lock == 'w'
                    self.locks[rid].release_exclusive_lock()

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