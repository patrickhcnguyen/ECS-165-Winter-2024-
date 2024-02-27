from lstore.table import Table
from lstore.Bufferpool import Bufferpool

class Database():

    def __init__(self):
        self.path = ''
        self.tables = []
        self.bufferpool = Bufferpool()
        pass

    # Not required for milestone1
    def open(self, path):
        self.path = path
        self.bufferpool.open(path)
        pass

    def close(self):
        self.bufferpool.close()
        self.tables.clear()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
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
