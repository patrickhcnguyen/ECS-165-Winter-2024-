from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
class Transaction:
    id = 0 # static id for each transaction
    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.held_locks = {}
        self.id = Transaction.id
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
            if query.__name__ == 'select':
                print("select")
                rids = table.index.locate(args[1], args[0])
                success = table.lock_manager.acquire_read_locks(rids)
                if not success:
                    print("cannot acquire S lock, another thread is writing") #PLEASE HANDLE THIS
                for rid in rids:
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('r')
            elif query.__name__ == 'select_version':
                print("select version")
                rids = table.index.locate(args[1], args[0])
                success = table.lock_manager.acquire_read_locks(rids)
                if not success:
                    print("cannot acquire S lock, another thread is writing") #PLEASE HANDLE THIS
                for rid in rids:
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('r')
            elif query.__name__ == 'update':
                print("update")
                key_col = table.key
                rid = table.index.locate(key_col, args[0])[0]
                if rid != []:
                    success = table.lock_manager.acquire_exclusive_lock(rid)
                    if success:
                        if rid not in self.held_locks:
                            self.held_locks[rid] = []
                        self.held_locks[rid].append('w')
                    else:
                        print("could not obtain X lock, another thread is reading/writing") #PLEASE HANDLE THIS
            elif query.___name__ == 'insert':
                #probably need to look out for phantom reads or something
                pass
            elif query.___name__ == 'delete':
                #probably need to check if any other thread is using it at the time of delete
                pass
            elif query.__name__ == 'sum':
                print("sum")
                key_col = table.key
                rids = self.index.locate_range(key_col, args[0], args[1])
                success = table.lock_manager.acquire_read_locks(rids)
                if not success:
                    print("cannot acquire S lock, another thread is writing") #PLEASE HANDLE THIS
                for rid in rids:
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('r')
            elif query.__name__ == 'sum_version':
                print("sum version")
                key_col = table.key
                rids = self.index.locate_range(key_col, args[0], args[1])
                success = table.lock_manager.acquire_read_locks(rids)
                if not success:
                    print("cannot acquire S lock, another thread is writing") #PLEASE HANDLE THIS
                for rid in rids:
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('r')
                pass
            elif query.__name__ == 'increment':
                #lock the whole table?
                pass
            else:
                print("insert for now")
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        table.lock_manager.release_all_locks(self.held_locks)
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):
        # TODO: commit to database
        return True
