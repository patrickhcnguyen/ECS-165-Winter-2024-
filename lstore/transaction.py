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
        print("transaction id:", self.id)
        for query, args, table in self.queries:
            if query.__name__ == 'select':
                print("select")
                rids = table.index.locate(args[1], args[0])
                table.lock_manager.acquire_read_locks(rids)
                for rid in rids:
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('r')
            elif query.__name__ == 'update':
                print("update")
                key_col = query.table.key
                rid = table.index.locate(key_col, args[0])[0]
                if rid != []:
                    table.lock_manager.acquire_exclusive_locks(rid)
                    if rid not in self.held_locks:
                        self.held_locks[rid] = []
                    self.held_locks[rid].append('w')
            else:
                print("prob insert")
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
