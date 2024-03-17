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
        self.commits = []
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
        self.commits.append(0)
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        i = 0
        for query, args, table in self.queries:
            if "table" in self.held_locks.keys(): #if a table lock is held by the transaction, all transactions will be able to get their locks by default, as the table lock won't be released until after commit
                success = True

            elif query.__name__ == 'select':
                #print("select")
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
                #print("update")
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

            elif query.__name__ == 'insert':
                #probably need to look out for phantom reads or something
                #check if table lock exists:
                success = table.lock_manager.block_table_lock(self.id)
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
                    args[0] = rid_val
                    if rid_val not in self.held_locks:
                        self.held_locks[rid_val] = []
                    self.held_locks[rid_val].append('w')
                else:
                    print("could not obtain X lock, another thread is reading/writing") #PLEASE HANDLE THIS
                    return self.abort()

            elif query.__name__ == 'sum': #scanning operation
                print("sum")
                success = table.lock_manager.acquire_table_lock(self.id)
                if not success:
                    print("cannot acquire scanning lock on table, outside transactions are holding locks") #PLEASE HANDLE THIS
                    return self.abort()
                self.held_locks["table"] = ['w']

            elif query.__name__ == 'sum_version': #scanning operation
                print("sum version")
                success = table.lock_manager.acquire_table_lock(self.id)
                if not success:
                    print("cannot acquire scanning lock on table, outside transactions are holding locks") #PLEASE HANDLE THIS
                    return self.abort()
                self.held_locks["table"] = ['w']

            elif query.__name__ == 'increment':
                key_col = table.key
                rid = table.index.locate(key_col, args[0])
                if rid == []:
                    return self.abort()
                rid_val = rid[0]
                success = table.lock_manager.acquire_exclusive_lock(rid_val, self.id)
                if success:
                    args[0] = rid_val
                    if rid_val not in self.held_locks:
                        self.held_locks[rid_val] = []
                    self.held_locks[rid_val].append('w')
                else:
                    print("could not obtain X lock, another thread is reading/writing") #PLEASE HANDLE THIS
                    return self.abort()

            else:
                print("Nothing....")

            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()

            if query.__name__ == 'insert':
                key_col = table.key
                rid = table.index.locate(key_col, args[key_col])[0]
                table.lock_manager.locks[rid].transaction_ids.append(self.id)
                self.held_locks[rid] = []
                self.held_locks[rid].append('w')

            self.commits[i] = 0
            i += 1
            
        return self.commit()

    
    def abort(self):
        print("aborted is happened ////////////////////////////////////")
        i = len(self.commits)-1
        for query, args, table in reversed(self.queries):
            if self.commits == 0:
                pass
            elif query.__name__ == 'insert':
                key_col = table.key
                rid = table.index.locate(key_col, args[key_col])[0]
                record = self.select_record(args[key_col], key_col, [1]*table.num_columns)[0]
                for i in range(len(record)):
                    table.index.delete_index(i, record[i], rid)
            elif query.__name__ == 'update':
                key_col = table.key
                rid = table.index.locate(key_col, args[1][key_col])[0] #must deference args this way in case primary_key was changed
                max_records = table.max_records
                page_set = rid // max_records 
                tail_rid = table.bufferpool.get_page(table.name, page_set*(table.num_columns+4), True).read_val(rid)
                tail_page_num = (tail_rid // max_records)*(table.num_columns+5)
                prev_version_rid = self.bufferpool.get_page(self.name, tail_page_num, False).read_val(tail_rid)  
                table.bufferpool.get_page(table.name, page_set*(table.num_columns+4), True).overwrite(prev_version_rid)
                data = self.select_record_version(args[key_col], key_col, [1]*table.num_columns, -1)[0]
                for i in range(len(data)):
                    if (table.index.indices[i] != None and args[i] != None):
                        table.index.delete_index(i, data[i], rid)
                        table.index.add_index(i, data[i], rid)
            elif query.__name__ == 'delete':
                rid =  args[0]
                max_records = table.max_records
                page_number = rid // max_records
                self.table.bufferpool.get_page(self.table.name, page_number * (self.table.num_columns + 4) + 2, True).overwrite(rid, 0)
                data = []
                for i in range(len(table.num_columns)):
                    data.append(self.table.bufferpool.get_page(self.table.name, page_number * (self.table.num_columns + 4) + 4 +i, True).read_val(rid))
                    if table.index.indices[i] != None:
                        table.index.add_index(i, data[i], rid)
            elif query.__name__ == 'increment': #increment simply creates an update/tail record but the arguments passed are different than those of the update function
                key_col = table.key
                rid = args[0]
                max_records = table.max_records
                page_set = rid // max_records 
                tail_rid = table.bufferpool.get_page(table.name, page_set*(table.num_columns+4), True).read_val(rid)
                tail_page_num = (tail_rid // max_records)*(table.num_columns+5)
                prev_version_rid = self.bufferpool.get_page(self.name, tail_page_num, False).read_val(tail_rid)
                incremented_data = self.bufferpool.get_page(self.name, tail_page_num+3+args[1], False).read_val(tail_rid)   
                table.bufferpool.get_page(table.name, page_set*(table.num_columns+4), True).overwrite(prev_version_rid)
                if (table.index.indices[args[1]] != None):
                    table.index.delete_index(args[1], incremented_data, rid)
                    table.index.add_index(args[1], incremented_data-1, rid)
            i -= 1
        for query, args, table in self.queries: 
            table.lock_manager.release_all_locks(self.held_locks, self.id)
        return False

    
    def commit(self):
        for query, args, table in self.queries: 
            table.lock_manager.release_all_locks(self.held_locks, self.id)
        return True
