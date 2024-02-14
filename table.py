from index import Index
from page import Page
from time import time
import struct


#last 4 columns of all records are listed below:
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
        self.key = key+4
        self.num_columns = num_columns
        self.page_directory = {}
        self.num_pages = -1 #stores the amount of pages minus 1
        self.init_page_dir(self.num_columns)
        self.index = Index(self)
        self.index.create_index(self.key)
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

    def update_record(self, key, *record): 
        key_rid = self.index.locate(self.key, key) #get the row number of the inputted key
        max_records = self.page_directory[0].max_records #this is defined in the page class as 64 records
        print(key_rid)
        page_set = key_rid[0] // max_records #select the page range that row falls in
        rid_in_page = key_rid[0]%max_records
        for i in range(self.num_columns):
            value = record[i]
            if (value == None):
                value = struct.unpack('i',self.page_directory[i+4+page_set*(self.num_columns+4)].data[rid_in_page*64:rid_in_page*64+struct.calcsize('i')])[0]
            self.page_directory[i+4+page_set*(self.num_columns+4)].write_update(value)
        
        #make the indirection column of the tail record hold the rid currently held in the base record's indirection column
        prev_version_rid = struct.unpack('i',self.page_directory[page_set*(self.num_columns+4)].data[rid_in_page*64:rid_in_page*64+struct.calcsize('i')])[0]
        tail_rid = self.page_directory[page_set*(self.num_columns+4)].write_update(prev_version_rid) #while inserting, use this chance to get the return value of write_update, the rid of the new tail record
        
        self.page_directory[1+page_set*(self.num_columns+4)].write_update(tail_rid) #Writing to the tail's rid column
        self.page_directory[2+page_set*(self.num_columns+4)].write_update(0)
        self.page_directory[3+page_set*(self.num_columns+4)].write_update(0)
        
        #update indirection column of base record
        packed_bytes = struct.pack('i', tail_rid)
        self.page_directory[page_set*(self.num_columns+4)].data[rid_in_page*64:rid_in_page*64+len(packed_bytes)] = packed_bytes
        return

    def insert_record(self, *columns):
        schema_encoding = '0' * self.num_columns

        latest_page = self.page_directory[self.num_pages]
        if latest_page.has_capacity() <= 0: #if there's no capacity
            self.init_page_dir(self.num_columns) #create new page range
        
        pages_start = (self.num_pages+1) - (self.num_columns+4)
        for i in range(self.num_columns):
            self.page_directory[i+4+pages_start].write(columns[i])
        self.page_directory[pages_start].write(-1) #indirection_column = -1 means no tail record exists
        self.page_directory[pages_start+1].write(self.rid) #rid column
        self.page_directory[pages_start+2].write(0) #time_stamp column
        self.page_directory[pages_start+3].write(0) #schema_encoding column
        self.index.add_index(self.key, columns[self.key], self.rid) # add index
        self.rid += 1

    def select(self, search_key, search_key_index, projected_columns_index):
        pass


