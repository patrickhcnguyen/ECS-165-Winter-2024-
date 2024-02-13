from index import Index
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
        self.index = Index(self)
        self.rid = 0
        pass

    def init_page_dir(self, num_columns): #adds one page-range of pages to the page_directory, if the base pages have filled up or to initialize the page directory
        for i in range(num_columns+4):
            self.num_pages += 1
            self.page_directory[self.num_pages] = Page() #each "Page" manages one base page and all tail pages to that one base page
        pass
    
    def __merge(self):
        print("merge is happening")
        pass

    def update_record(self, key, record): #for updating the indirection column of the base record, write_update returns the rid of the new tail record 
        pass

    def insert_record(self, *columns):
        schema_encoding = '0' * self.table.num_columns

        latest_page = self.page_directory[-self.num_columns]
        if latest_page.has_capacity() <= 0: #if there's no capacity
            self.init_page_dir(self.num_columns) #create new page range
        
        self.table.page_directory[self.num_pages - self.num_columns].write(self.rid)
        self.table.page_directory[self.num_pages - self.num_columns + 1].write(self.rid)
        self.table.page_directory[self.num_pages - self.num_columns + 2].write(0)
        self.table.page_directory[self.num_pages - self.num_columns + 3].write(schema_encoding)
        for i in range(self.num_pages - self.num_columns + 4, self.num_pages):
            self.table.page_directory[i].write(columns[i])

        self.table.index.add_index(RID_COLUMN, columns[self.key], self.rid) # add index
        self.rid += 1


