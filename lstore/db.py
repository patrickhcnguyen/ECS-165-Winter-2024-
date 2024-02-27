from lstore.table import Table
from lstore.Bufferpool import BufferPool
import os

class Database():

    def __init__(self):
        self.path = ''
        self.tables = []
        self.table_paths = []
        self.bufferpool = BufferPool()
        pass

    # Not required for milestone1
    def open(self, path):
        self.path = path
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        #self.bufferpool.open(path)
        pass

    def close(self):
        #self.bufferpool.close()
        self.tables.clear()
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
        self.table_paths.append(path)
        table = Table(name, num_columns, key_index, path, self.bufferpool)
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
        pass
