from lstore.index import Index
from lstore.page import Page
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
        self.num_columns = num_columns #excludes the 4 columns written above

        self.page_directory = {}
        self.tail_page_directory = {}
        self.num_pages = -1 #stores the amount of pages minus 1
        self.num_tail_pages = -1 #stores the amount of pages minus 1
        self.init_page_dir()
        self.init_tail_page_dir()

        self.index = Index(self)
        self.index.create_index(self.key)
        self.rid = 0
        pass

    def init_page_dir(self): #adds one set of physical pages to the page_directory, in case the base pages have filled up or to initialize the page directory
        for i in range(self.num_columns+4):
            self.num_pages += 1
            self.page_directory[self.num_pages] = Page()
        pass

    def init_tail_page_dir(self): #adds one set of physical pages to the tail_page_directory, in case the tail pages have filled up or to initialize the tail page directory
        for i in range(self.num_columns+4):
            self.num_tail_pages += 1
            self.tail_page_directory[self.num_tail_pages] = Page()
        pass
    
    def __merge(self):
        print("merge is happening")
        pass


    def update_record(self, key, *record): 
        key_rid = (self.index.locate(self.key, key))[0] #get the row number of the inputted key
        max_records = self.page_directory[0].max_records #this is defined in the page class as 64 records
        page_set = key_rid // max_records #select the base page (page set) that row falls in
        
        latest_tail_page = self.tail_page_directory[self.num_tail_pages]
        if latest_tail_page.has_capacity() <= 0: #if there's no capacity
            self.init_tail_page_dir() #add one tail page (a set of physical pages, one for each column)
        pages_start = (self.num_tail_pages+1) - (self.num_columns+4)

        # write the first 4 columns of the tail record: indirection column, rid, schema_encoding, and time_stamp
        # make the indirection column of the tail record hold the rid currently held in the base record's indirection column
            # tail record of indirection column will then point to the prev version of data -> will be -1 if the prev version is the base record, based on our implementation of insert_record
        prev_version_rid = self.page_directory[page_set*(self.num_columns+4)].read_val(key_rid)
        index_within_page = self.tail_page_directory[pages_start].write(prev_version_rid) #while inserting, use this chance to get the return value of write_update, the rid of the new tail record
        tail_rid = index_within_page + (pages_start // (self.num_columns+4))*(max_records)

        self.tail_page_directory[1+pages_start].write(tail_rid) #Writing to the tail's rid column
        self.tail_page_directory[2+pages_start].write(0)
        self.tail_page_directory[3+pages_start].write(0)
        
        # write the actual data columns of the tail record
        if (prev_version_rid == -1): # reference the base record during the update
            for i in range(self.num_columns):
                value = record[i]
                if (value == None):
                    value = self.page_directory[i+4+page_set*(self.num_columns+4)].read_val(key_rid)
                self.tail_page_directory[i+4+pages_start].write(value)
        else: # reference the prev_tail_record during the update
            prev_tpage_set = prev_version_rid // max_records
            prev_trid_in_page = prev_version_rid % max_records
            for i in range(self.num_columns):
                value = record[i]
                if (value == None):
                    value = self.tail_page_directory[i+4+prev_tpage_set*(self.num_columns+4)].read_val(prev_version_rid)
                self.tail_page_directory[i+4+pages_start].write(value)

        #update indirection column of base record
        self.page_directory[page_set*(self.num_columns+4)].overwrite(key_rid, tail_rid)
        #update schema encoding column of base record
        self.page_directory[page_set*(self.num_columns+4)].read_val(1)
        return


    def insert_record(self, *columns):
        schema_encoding = '0' * self.num_columns

        latest_page = self.page_directory[self.num_pages]
        if latest_page.has_capacity() <= 0: #if there's no capacity
            self.init_page_dir() #add one base page (a set of physical pages, one for each column)
        
        pages_start = (self.num_pages+1) - (self.num_columns+4)
        for i in range(self.num_columns):
            self.page_directory[i+4+pages_start].write(columns[i])
        self.page_directory[pages_start].write(-1) #indirection_column = -1 means no tail record exists
        self.page_directory[pages_start+1].write(self.rid) #rid column
        self.page_directory[pages_start+2].write(0) #time_stamp column
        self.page_directory[pages_start+3].write(0) #schema_encoding column
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
                    columns.append(data)
        else: # has been updated, get tail page (return record in tail page)
            tail_page = indirection // max_records
            tail_page_index = indirection % max_records
            for i in range(len(projected_columns_index)):
                if projected_columns_index[i] == 1:
                    page = self.page_directory[base_page_index + i + 4]
                    data = struct.unpack('i', page.tailPage_directory[tail_page]["page"][tail_page_index*64:tail_page_index*64+struct.calcsize('i')])[0] # other version: change to page_directory[base_page_index + i + self.num_columns]
                    columns.append(data)

        new_record = Record(key_rid, search_key, columns)
        record_list = []
        record_list.append(new_record)
        return record_list
    
    def sum_records(self, start, end, column_index):
        total_sum = 0
        max_records = self.page_directory[0].max_records #64 records
        # get all rid's within list
        rid_list = self.index.locate_range(self.key, start, end)

        for rid in rid_list:
            base_record_index = rid % max_records
            base_page_index = (rid // max_records)*(self.num_columns+4)
            indirection_page = self.page_directory[base_page_index] # other version: change to only base_page_index
            indirection = struct.unpack('i',indirection_page.data[base_record_index*64:base_record_index*64+struct.calcsize('i')])[0]
            if indirection == -1: # has not been updated (return record in base page)
                page = self.page_directory[base_page_index + column_index + 4]
                data = struct.unpack('i',page.data[base_record_index*64:base_record_index*64+struct.calcsize('i')])[0]
                # print(data)
                total_sum += data
            else: # has been updated, get tail page (return record in tail page)
                tail_page = indirection // max_records
                tail_page_index = indirection % max_records
                page = self.page_directory[base_page_index + column_index + 4]
                data = struct.unpack('i', page.tailPage_directory[tail_page]["page"][tail_page_index*64:tail_page_index*64+struct.calcsize('i')])[0] # other version: change to page_directory[base_page_index + i + self.num_columns]
                # print(data)
                total_sum += data
        
        return total_sum


