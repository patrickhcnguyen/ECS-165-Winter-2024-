import threading

class LockManager:
    def __init__(self):
        self.locks = {} # key: rid, value: lock
        self.thread_lock = threading.Lock() #this variable captures whichever thread accesses this LockManager object first (removing race condition to acquire the locks)

    """
    parameters:
    rid_list - list of rid's to acquire read locks
    """
    def acquire_read_locks(self, rid_list, t_id=None):
        with self.thread_lock:
            if "table" in self.locks:
                return False
            for rid in rid_list:
                if rid not in self.locks:
                    self.locks[rid] = Lock()
                return self.locks[rid].get_shared_lock(t_id)
                #print("rid: ", rid, "read_count", self.locks[rid].read_count)
            return True

    def acquire_exclusive_lock(self, rid, t_id=None):
        with self.thread_lock:
            if "table" in self.locks:
                return False
            if rid in self.locks:
                pass
            else:
                self.locks[rid] = Lock()
            return self.locks[rid].get_exclusive_lock(t_id)


    def acquire_table_lock(self, t_id):
        with self.thread_lock:
            check_compatibility = True #checks whetehr the other locks existing are of the same transaction
            for lock in self.locks:
                for i in lock.transaction_ids:
                    if i != t_id:
                        check_compatibility = False
            if check_compatibility == False:
                return False
            
            self.locks["table"] = Lock()
            return self.locks["table"].get_exclusive_lock(t_id)

    def block_table_lock(self, t_id):
        with self.thread_lock:
            if "table" in self.locks:
                return False
            else:
                if "dynamic-state" not in self.locks:
                    self.locks["dynamic-state"] = Lock()
                return self.locks["dynamic-state"].get_shared_lock(t_id) #get a shared lock so other transactions that are inserting can also do so, this lock is to simply stop scanning operations

    def release_all_locks(self, held_locks, t_id):
        print("release locks")
        with self.thread_lock:
            for rid, locks in held_locks.items():
                # print("rid: ", rid, " locks: ", locks)
                for lock in locks:
                    if lock == 'r':
                        self.locks[rid].release_shared_lock()
                    elif lock == 'w':
                        self.locks[rid].release_exclusive_lock()

class Lock:
    def __init__(self):
        self.read_count = 0
        self.write_count = 0
        self.transaction_ids = []

    # read-lock: other transactions can only read but not write
    def get_shared_lock(self, t_id=None): #read lock
        if self.write_count == 0:
            self.read_count += 1
            if t_id!=None:
                self.transaction_ids.append(t_id)
            return True
        
        check_compatibility = True #check if t_id currently holds an X on this lock
        for i in self.transaction_ids:
            if i != t_id:
                check_compatibility = False
        if check_compatibility == True:
            self.read_count += 1
            if t_id!=None:
                self.transaction_ids.append(t_id)
            return True
        return False

    # write-lock: other transactions cannot read or write
    def get_exclusive_lock(self, t_id=None):
        if self.read_count == 0 and self.write_count == 0:
            self.write_count += 1
            if t_id!=None:
                self.transaction_ids.append(t_id)
            return True
        
        if t_id == None: #nothingmore can be done without knowing a transaction id :(
            return False
        
        check_compatibility = True #check if t_id currently holds an S or X lock and can be upgraded to an X lock (no other transactions are holding this lock)
        for i in self.transaction_ids:
            if i != t_id:
                check_compatibility = False
        if check_compatibility == True:
            self.write_count += 1
            if t_id!=None:
                self.transaction_ids.append(t_id)
            return True
        return check_compatibility

    def release_shared_lock(self, t_id=None):
        if self.read_count > 0:
            self.read_count -= 1
            if t_id!=None:
                try:
                    self.transaction_ids.remove(t_id)
                except ValueError:
                    pass
            return True
        return False

    def release_exclusive_lock(self, t_id=None):
        if self.write_count > 0:
            self.write_count -= 1
            if t_id!=None:
                try:
                    self.transaction_ids.remove(t_id)
                except ValueError:
                    pass
            return True
        return False