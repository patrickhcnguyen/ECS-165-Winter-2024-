from lstore.index import Index
from time import time

#first 4 columns of all records are listed below:
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table: 

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.init_page_dir()
        self.index = Index(self)
        pass

    def init_page_dir(self, num_columns):
        for i in range(num_columns+3):
            self.page_directory.append(key: i, page: Page())
        pass

    def __merge(self):
        print("merge is happening")
        pass