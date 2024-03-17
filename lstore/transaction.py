from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
import threading

class Transaction:
    id = 0 # static id for each transaction
    thread_lock = threading.Lock()
    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.held_locks = {}
        self.id = 0
        with Transaction.thread_lock:
            self.id = Transaction.id
            self.increment_id()
        self.increment_id()
        pass

    def increment_id(self):
        Transaction.id += 1

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args, table))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args, table in self.queries:
            if "table" in self.held_locks.keys(): #if a table lock is held by the transaction, all transactions will be able to get their locks by default, as the table lock won't be released until after commit
                success = True

            elif query.__name__ == 'select':
                print("select")
                rids = table.index.locate(args[1], args[0])
                success = table.lock_manager.acquire_read_locks(rids, self.id)
                if not success:
                    print("cannot acquire S lock, another thread is writing") #PLEASE HANDLE THIS
                    return self.abort()
                for rid in rids:
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('r')
                
            elif query.__name__ == 'select_version':
                print("select version")
                rids = table.index.locate(args[1], args[0])
                success = table.lock_manager.acquire_read_locks(rids, self.id)
                if not success:
                    print("cannot acquire S lock, another thread is writing") #PLEASE HANDLE THIS
                    return self.abort()
                for rid in rids:
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('r')

            elif query.__name__ == 'update':
                print("update")
                key_col = table.key
                rid = table.index.locate(key_col, args[0])
                if rid != []:
                    rid_val = rid[0]
                success = table.lock_manager.acquire_exclusive_lock(rid_val, self.id)
                if success:
                    if rid_val not in self.held_locks:
                        self.held_locks[rid_val] = []
                    self.held_locks[rid_val].append('w')
                else:
                    print("could not obtain X lock, another thread is reading/writing") #PLEASE HANDLE THIS
                    return self.abort()

            elif query.__name__ == 'insert':
                #probably need to look out for phantom reads or something
                #check if table lock exists:
                success = table.lock_manager.block_table_lock(self.id)
                #print("insert")
                if success:
                    if "dynamic-state" not in self.held_locks:
                        self.held_locks["dynamic-state"] = []
                    self.held_locks["dynamic-state"].append('r')
                else:
                    print("could not insert, table lock exists") #PLEASE HANDLE THIS
                    return self.abort()
                

            elif query.__name__ == 'delete':
                #probably need to check if any other thread is using it at the time of delete
                print("delete")
                key_col = table.key
                rid = table.index.locate(key_col, args[0])
                if rid == []:
                    return self.abort()
                rid_val = rid[0]
                success = table.lock_manager.acquire_exclusive_lock(rid_val, self.id)
                if success:
                    if rid_val not in self.held_locks:
                        self.held_locks[rid_val] = []
                    self.held_locks[rid_val].append('w')
                else:
                    print("could not obtain X lock, another thread is reading/writing") #PLEASE HANDLE THIS
                    return self.abort()

            elif query.__name__ == 'sum':
                print("sum")
                key_col = table.key
                rids = self.index.locate_range(key_col, args[0], args[1])
                success = table.lock_manager.acquire_read_locks(rids, self.id)
                if not success:
                    print("cannot acquire S lock, another thread is writing") #PLEASE HANDLE THIS
                    return self.abort()
                for rid in rids:
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('r')

            elif query.__name__ == 'sum_version':
                print("sum version")
                key_col = table.key
                rids = self.index.locate_range(key_col, args[0], args[1])
                success = table.lock_manager.acquire_read_locks(rids, self.id)
                if not success:
                    print("cannot acquire S lock, another thread is writing") #PLEASE HANDLE THIS
                    return self.abort()
                for rid in rids:
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('r')

            elif query.__name__ == 'increment': #increment is guarenteed to go through, it will lock the whole table and the other queries of the transaction can use this table lock, which can not even be released until the end of the transaction:
                success = table.lock_manager.acquire_table_lock(self.id)
                if not success:
                    print("cannot acquire W lock on table, another transaction is holding locks") #PLEASE HANDLE THIS
                    return self.abort()
                self.held_locks["table"] = ['t']

            else:
                print("insert for now")
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            if query.__name__ == 'insert':
                key_col = table.key
                rid = table.index.locate(key_col, args[key_col])[0]
                table.lock_manager.locks[rid].append(self.id)
                self.held_locks[rid] = []
                self.held_locks[rid].append('w')
        table.lock_manager.release_all_locks(self.held_locks)
        return self.commit()

    
    def abort(self):
        for query, args, table in reversed(self.queries):
            #mark all changed rows as "needs deletion"
            #if insert
                #find rid given the primary key using index locate
                #mark the inserted row for deletion
                #remove index
            #if update:
                #find base-rid given the primary key using index locate
                #place special marker for the updated row to be ignored in select, sum, and merge
            #if delete:
                #remove deletion marker
                #add index back
            if query.__name__ == 'insert':
                key_col = table.key
                rid = table.index.locate(key_col, args[key_col])[0]
                #table.index.delete_index(rid)

        for query, args, table in self.queries: 
            table.lock_manager.release_all_locks(self.held_locks)
        return False

    
    def commit(self):
        # TODO: commit to database
        return True
