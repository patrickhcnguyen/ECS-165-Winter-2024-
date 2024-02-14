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
        page_set = key_rid[0] // max_records #select the page range that row falls in
        rid_in_page = key_rid[0]%max_records

        for i in range(self.num_columns):
            value = record[i]
            if (value == None):
                value = struct.unpack('i',self.page_directory[i+4+page_set*(self.num_columns+4)].data[rid_in_page*64:rid_in_page*64+struct.calcsize('i')])[0]
            self.page_directory[i+4+page_set*(self.num_columns+4)].write_update(value)
        
        # make the indirection column of the tail record hold the rid currently held in the base record's indirection column
            # tail record of indirection column points to prev version of data -> will be -1 if the prev version is the base record, due to our implementation of insert_record
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
        print(columns[self.key-4])
        self.index.add_index(self.key, columns[self.key-4], self.rid) # add index
        self.rid += 1

    def select_record(self, search_key, search_column, projected_columns_index):
        # get index with search_key
        # go to base page of record
        # check indirection column (whether it has been updated or not)
        # if it has been updated, go to tail page and find the record
        # if it has not been updated, retrieve the projected_columns 
        # create a record with the info and return it

        key_rid = self.index.locate(self.key, search_key)
        max_records = self.page_directory[0].max_records #64 records
        base_record_index = key_rid[0] % max_records
        base_page_index = (key_rid[0] // max_records)*(self.num_columns+4)
        indirection_page = self.page_directory[base_page_index] # other version: change to only base_page_index
        indirection = struct.unpack('i',indirection_page.data[base_record_index*64:base_record_index*64+struct.calcsize('i')])[0]
        columns = []
        if indirection == -1: # has not been updated (return record in base page)
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    page = self.page_directory[base_page_index + i + 4]
                    data = struct.unpack('i',page.data[base_record_index*64:base_record_index*64+struct.calcsize('i')])[0]
                    print(data)
                    columns.append(data)
        else: # has been updated, get tail page (return record in tail page)
            tail_page = indirection // max_records
            tail_page_index = indirection % max_records
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    page = self.page_directory[base_page_index + i + 4]
                    data = struct.unpack('i', page.tailPage_directory[tail_page]["page"][tail_page_index*64:tail_page_index*64+struct.calcsize('i')])[0] # other version: change to page_directory[base_page_index + i + self.num_columns]
                    print("geting record data: ",data)
                    columns.append(data)

        new_record = Record(key_rid, search_key, columns)
        record_list = []
        record_list.append(new_record)
        return record_list


