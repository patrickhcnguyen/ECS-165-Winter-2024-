from lstore.table import Table
from lstore.Bufferpool import BufferPool
import os
import pickle
from lstore.lock import Lock, LockManager
import threading
import shutil

class Database():

    def __init__(self):
        self.path = ''
        self.tables = {}
        self.table_paths = {}
        self.table_columns = {}
        self.bufferpool = BufferPool()
        pass

    # Not required for milestone1
    def open(self, path):
        self.path = path
        if not os.path.exists(self.path):
            os.makedirs(self.path)
            self.bufferpool.parent_path = self.path
            return
        
        filename = "dbdata.pickle"
        path = os.path.join(self.path, filename)
        if not os.path.exists(path): #if dbdata.pickle does not exist, db was not closed properly last... Just make a new db
            shutil.rmtree(self.path, ignore_errors=True)
            os.makedirs(self.path)
            self.bufferpool.parent_path = self.path
            return
        with open(path, 'rb') as f:
            self.table_columns = pickle.load(f)
        self.bufferpool.parent_path = self.path
        self.bufferpool.open()

        for file in os.listdir(self.path):
            if file!="bufferpool.pickle" and file!="dbdata.pickle":
                num_columns = self.table_columns[file]
                path = os.path.join(self.path, file)
                table = Table(file, num_columns, 0, path, self.bufferpool)
                table.open()
                table.lock_manager = LockManager()
                table.thread_lock = threading.Lock()
                table.merge_thread_lock = threading.Lock()
                self.tables[file] = table
                self.table_paths[file] = path
                self.bufferpool.add_table(file, table)
        return


    def close(self):
        self.bufferpool.close()
        for key in self.tables.keys():
            self.tables[key].lock_manager = None
            self.tables[key].thread_lock = None
            self.tables[key].merge_thread_lock = None
            self.tables[key].close()
        self.tables.clear()

        filename = "dbdata.pickle"
        path = os.path.join(self.path, filename)
        with open(path, 'wb') as f:
            pickle.dump(self.table_columns, f) #dump all metadata, pagedirectory, and index 
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        parent_dir = self.path
        directory = name
        path = os.path.join(parent_dir, directory)
        if not os.path.exists(path):
            os.makedirs(path)
        self.table_paths[name] = path
        table = Table(name, num_columns, key_index, path, self.bufferpool)
        self.tables[name] = table
        self.table_columns[name] = num_columns
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
