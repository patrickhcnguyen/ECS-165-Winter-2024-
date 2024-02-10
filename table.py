#from index import Index
from page import Page
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
        self.num_pages = -1 #stores the amount of pages minus 1
        self.init_page_dir(self.num_columns)
        #self.index = Index(self)
        pass

    def init_page_dir(self, num_columns): #adds one page-range of pages to the page_directory, if the base pages have filled up or to initialize the page directory
        for i in range(num_columns+3):
            self.num_pages += 1
            self.page_directory[self.num_pages] = Page() #each "Page" manages one base page and all tail pages to that one base page
        pass
        
    def __merge(self):
        print("merge is happening")
        pass

    def update_record(self, key, record): #for updating the indirection column of the base record, write_update returns the rid of the new tail record 
        pass