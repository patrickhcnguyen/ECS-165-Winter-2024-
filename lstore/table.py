from lstore.index import Index
from lstore.page import Page
from lstore.Bufferpool import BufferPool
from time import time
import struct
import os
import pickle


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
    def __init__(self, name, num_columns, key, path='none', bufferpool='none'):
        self.name = name
        self.key = key
        self.num_columns = num_columns #excludes the 4 columns written above
        self.max_records = 64 #the max_records able to be stored in one page, this MUST mirror max_records from page class

        #organize folders related to this table
        self.path = path
        if path == 'none':
            self.path = "./"+name
            os.mkdir(self.path)
        directory = "base_pages"
        self.base_path = os.path.join(self.path, directory)
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path) 
        directory = "tail_pages"
        self.tail_path = os.path.join(self.path, directory)
        if not os.path.exists(self.tail_path):
            os.makedirs(self.tail_path)

        self.bufferpool = bufferpool
        if bufferpool == 'none':
            self.bufferpool = BufferPool()
        self.bufferpool.add_table(self.name, self)

        self.page_directory = {}
        self.tail_page_directory = {}
        self.num_pages = -1 #stores the amount of pages minus 1
        self.num_tail_pages = -1 #stores the amount of pages minus 1
        self.init_page_dir()
        self.init_tail_page_dir()

        self.index = Index(self)
        for i in range(self.num_columns):
            self.index.create_index(i)
        self.rid = 0  #rid of the next spot in the page range (not of the latest record)
        
        self.total_tail_records = 0
        self.tps = 0 # INDEX FIX: Should be an array that represents each column

        pass

    def init_page_dir(self): #adds one set of physical pages to the page_directory, in case the base pages have filled up or to initialize the page directory
        for i in range(self.num_columns+4):
            self.num_pages += 1
            page = Page()
            self.bufferpool.addPages(self.name, page, self.num_pages, True)
        pass

    def init_tail_page_dir(self): #adds one set of physical pages to the tail_page_directory, in case the tail pages have filled up or to initialize the tail page directory
        for i in range(self.num_columns+5):
            self.num_tail_pages += 1
            page = Page()
            self.bufferpool.addPages(self.name, page, self.num_tail_pages, False)
        pass
    
    

    def __merge(self): #FIX: change to __merge after testing for bufferpool
        #print("merge is happening...")
        # tail_records = self.tail_page_directory.copy() # BUFFERPOOL FIX: obtain copies from disk of all tail records
        tail_records = self.bufferpool.get_tail_pages(self.name)
        base_page_copies = {}
        updatedQueue = set()
        max_records = self.max_records
        for i in reversed(range(self.total_tail_records - self.tps)): #ex. if there are 40 tail records (latest rid=39) and tps at rid=27 (tail-record with rid=27 and beyond still need to be merged), then creates a range from 12 to 0
            tail_rid = i + self.tps
            tail_page_index = (tail_rid // max_records)*(self.num_columns + 5) #tail page now has a 5th column, base-rid
            base_rid = tail_records[tail_page_index + 4 + self.num_columns].read_val(tail_rid)
            base_page_index = (base_rid // max_records)*(self.num_columns + 4)

            #print("base id", base_rid)
            if base_rid not in updatedQueue: # skips merge if base page rid is already in updated_queue
                #print("merged")
                for i in range(self.num_columns): # replaces values of base record with latest tail record
                    value = tail_records[tail_page_index + 4 + i].read_val(tail_rid)
                    base_page = None
                    if (base_page_index + 4 + i) in base_page_copies: # if page has been stored, retrieve it from memory
                        base_page = base_page_copies[base_page_index + 4 + i]
                    else: # else retrieve it from disk
                        # base_page = self.page_directory[base_page_index + 4 + i].copy() #BUFFERPOOL FIX: obtain copy from disk 
                        base_page = self.bufferpool.get_page(self.name, base_page_index + 4 + i).copy()
                        base_page_copies[base_page_index + 4 + i] = base_page
                    base_page.overwrite(base_rid, value)

                # in place updated for metadata
                if (base_page_index + 3) in base_page_copies: # if page has been stored, retrieve it from memory
                    base_page_copies[base_page_index + 3].overwrite(base_rid, 0)
                else: # else retrieve it from disk
                    base_page = self.bufferpool.get_page(self.name, base_page_index + 3).copy()
                    # base_page = self.page_directory[base_page_index + 3].copy() #BUFFERPOOL FIX: obtain copy from disk 
                    base_page_copies[base_page_index + 3] = base_page
                    base_page.overwrite(base_rid, 0)
            updatedQueue.add(base_rid)
        for page_num in base_page_copies:
            self.bufferpool.replace_page(self.name, page_num, base_page_copies[page_num])
            # self.page_directory[page_num] = base_page_copies[page_num] #BUFFERPOOL FIX: push updated pages back into disk and bufferpool

        self.tps = self.total_tail_records

    # QUERY FUNCTIONS
    def update_record(self, key, *record):
        key_rid = (self.index.locate(self.key, key))[0] #get the row number of the inputted key
        for i in range(len(record)):
            self.index.delete_index(i, record[i], key_rid)
            self.index.add_index(i, record[i], key_rid)
        max_records = self.max_records #this is defined in the page class as 64 records
        page_set = key_rid // max_records #select the base page (row of physical pages) that row falls in
        if (self.total_tail_records%(max_records*20)==0): #merge if the current total_tail_records has filled up 5 tail-pages more than since the last merge
            self.__merge()

        self.total_tail_records += 1
        latest_tail_page = self.bufferpool.get_page(self.name, self.num_tail_pages, False)
        if latest_tail_page.has_capacity() <= 0: #if there's no capacity
            self.init_tail_page_dir() #add one tail page (a set of physical pages, one for each column)
        pages_start = (self.num_tail_pages+1) - (self.num_columns+5)

        # write the first 4 columns of the tail record: indirection column, rid, schema_encoding, and time_stamp
        # make the indirection column of the tail record hold the rid currently held in the base record's indirection column
            # tail record of indirection column will then point to the prev version of data -> will be -1 if the prev version is the base record, based on our implementation of insert_record
        prev_version_rid = self.bufferpool.get_page(self.name, page_set*(self.num_columns+4), True).read_val(key_rid)
        index_within_page = self.bufferpool.get_page(self.name, pages_start, False).write(prev_version_rid) #while inserting, use this chance to get the return value of write_update, the rid of the new tail record
        #print("indirection column should be ",)
        tail_rid = index_within_page + (pages_start // (self.num_columns+5))*(max_records)
        self.bufferpool.get_page(self.name, 1+pages_start, False).write(tail_rid) #Writing to the tail's rid column
        self.bufferpool.get_page(self.name, 2+pages_start, False).write(0)
        self.bufferpool.get_page(self.name, 3+pages_start, False).write(0)
        
        # write the actual data columns of the tail record
        if (prev_version_rid == -1): # reference the base record during the update
            for i in range(self.num_columns):
                value = record[i]
                if (value == None):
                    value = self.bufferpool.get_page(self.name, i+4+page_set*(self.num_columns+4), True).read_val(key_rid)
                self.bufferpool.get_page(self.name, i+4+pages_start, False).write(value)
        else: # reference the prev_tail_record during the update
            prev_tpage_set = prev_version_rid // max_records
            for i in range(self.num_columns):
                value = record[i]
                if (value == None):
                    value = self.bufferpool.get_page(self.name, i+4+prev_tpage_set*(self.num_columns+5), False).read_val(prev_version_rid)
                    #print("get val from indirection: ", value)
                page0 = self.bufferpool.get_page(self.name, pages_start, False)
                self.bufferpool.get_page(self.name, i+4+pages_start, False).write(value)
                num_records=page0.num_records
                #print("p", page0.read_val(num_records-1))
        self.bufferpool.get_page(self.name, self.num_columns+4+pages_start, False).write(key_rid)
        #update indirection column of base record
        self.bufferpool.get_page(self.name, page_set*(self.num_columns+4), True).overwrite(key_rid, tail_rid)
        #update schema encoding column of base record
        self.bufferpool.get_page(self.name, 3+page_set*(self.num_columns+4), True).overwrite(key_rid, 1)
        return


    def insert_record(self, *columns):
        schema_encoding = '0' * self.num_columns

        latest_page = self.bufferpool.get_page(self.name, self.num_pages, True)
        if latest_page.has_capacity() <= 0: #if there's no capacity
            self.init_page_dir() #add one base page (a set of physical pages, one for each column)
        
        pages_start = (self.num_pages+1) - (self.num_columns+4)
        for i in range(self.num_columns):
            self.bufferpool.get_page(self.name, i+4+pages_start, True).write(columns[i])
        self.bufferpool.get_page(self.name, pages_start, True).write(-1) #indirection_column = -1 means no tail record exists
        self.bufferpool.get_page(self.name, pages_start+1, True).write(self.rid) #rid column
        self.bufferpool.get_page(self.name, pages_start+2, True).write(0) #time_stamp column
        self.bufferpool.get_page(self.name, pages_start+3, True).write(0) #schema_encoding column
        self.index.add_index(self.key, columns[self.key], self.rid) # add index
        for i in range(self.num_columns):
            self.index.add_index(i, columns[i], self.rid)
        self.rid += 1

    def select_record(self, search_key, search_column, projected_columns_index):
        # get index with search_key
        # go to base page of record
        # check indirection column (whether it has been updated or not)
        # if it has been updated, go to tail page and find the record
        # if it has not been updated, retrieve the projected_columns 
        # create a record with the info and return it
        record_list = []
        key_rid = self.index.locate(search_column, search_key)
        for key in key_rid:
            max_records = self.max_records #64 records
            base_page_index = (key // max_records)*(self.num_columns+4)
            indirection = self.bufferpool.get_page(self.name, base_page_index, True).read_val(key) # other version: change to only base_page_index
            columns = []
            if indirection == -1 or indirection < self.tps: # has not been updated (return record in base page)
                for i in range(len(projected_columns_index)):
                    if projected_columns_index[i] == 1:
                        data = self.bufferpool.get_page(self.name, base_page_index+i+4, True).read_val(key)
                        columns.append(data)
            else: # has been updated, get tail page (return record in tail page)
                tail_page_index = (indirection // max_records)*(self.num_columns+5)
                for i in range(len(projected_columns_index)):
                    if projected_columns_index[i] == 1:
                        data = self.bufferpool.get_page(self.name, tail_page_index+i+4, False).read_val(indirection)
                        columns.append(data)
            new_record = Record(key, search_key, columns)
            print(new_record.columns)
            record_list.append(new_record)
        return record_list
    
    def select_record_version(self, search_key, search_column, projected_columns_index, version_num):
        record_list = []
        key_rid = self.index.locate(search_column, search_key)
        for key in key_rid:
            max_records = self.max_records #64 records
            base_page_index = (key // max_records)*(self.num_columns+4)
            indirection = self.bufferpool.get_page(self.name, base_page_index, True).read_val(key) # other version: change to only base_page_index
            # print(indirection)
            columns = []
            if indirection == -1 or indirection < self.tps: # has not been updated (return record in base page)
                for i in range(len(projected_columns_index)):
                    if projected_columns_index[i] == 1:
                        data = self.bufferpool.get_page(self.name, base_page_index+i+4, True).read_val(key)
                        columns.append(data)
            else: # has been updated, get tail page (return record in tail page with correct version)
                tail_page_index = (indirection // max_records)*(self.num_columns+5)
                counter = -version_num # how many times we have to go back
                has_past = True # if there is more versions before the current tail record
                while(counter > 0 and has_past): # keep going back until it reaches the desired version
                    tail_page_index = (indirection // max_records)*(self.num_columns+5)
                    indirection = self.bufferpool.get_page(self.name, tail_page_index, False).read_val(indirection)
                    counter -= 1
                    if indirection == -1 or indirection < self.tps:
                        has_past = False
                if has_past:
                    for i in range(len(projected_columns_index)):
                        if projected_columns_index[i] == 1:
                            data = self.bufferpool.get_page(self.name, tail_page_index+i+4, False).read_val(indirection)
                            columns.append(data)
                else: # if it's asking for versions that doesn't exist, return base page
                    for i in range(len(projected_columns_index)):
                        if projected_columns_index[i] == 1:
                            data = self.bufferpool.get_page(self.name, base_page_index+i+4, True).read_val(key)
                            columns.append(data)
            new_record = Record(key, search_key, columns)
            record_list.append(new_record)
            #print(new_record.columns)
            key = self.index.locate(0, 92106477)[0]
            base_page_index = (key // max_records)*(self.num_columns+4)     
            #print(self.page_directory[base_page_index])       
        return record_list

    
    def sum_records(self, start, end, column_index):
        total_sum = 0
        max_records = self.max_records #64 records
        # get all rid's within list
        rid_list = self.index.locate_range(self.key, start, end)
        if len(rid_list) == 0:
            return None

        for rid in rid_list:
            base_page_index = (rid // max_records)*(self.num_columns+4)
            indirection = self.bufferpool.get_page(self.name, base_page_index, True).read_val(rid) # other version: change to only base_page_index
            if indirection == -1 or indirection < self.tps: # has not been updated (return record in base page)
                data = self.bufferpool.get_page(self.name, base_page_index+column_index+4, True).read_val(rid)
                total_sum += data
            else: # has been updated, get tail page (return record in tail page)
                tail_page_index = (indirection // max_records)*(self.num_columns+5)
                data = self.bufferpool.get_page(self.name, tail_page_index+column_index+4, False).read_val(indirection)
                total_sum += data
        
        return total_sum

    def sum_records_version(self, start, end, column_index, version_num):
        total_sum = 0
        max_records = self.max_records #64 records
        # get all rid's within list
        rid_list = self.index.locate_range(self.key, start, end)
        if len(rid_list) == 0:
            return None
        for rid in rid_list:
            base_page_index = (rid // max_records)*(self.num_columns+4)
            indirection = self.bufferpool.get_page(self.name, base_page_index, True).read_val(rid)
            if indirection == -1 or indirection < self.tps: # has not been updated (return record in base page)
                data = self.bufferpool.get_page(self.name, base_page_index+column_index+4, True).read_val(rid)
                total_sum += data
            else: # has been updated, get tail page (return record in tail page)
                counter = -version_num # how many times we have to go back
                has_past = True # if there is more versions before the current tail record
                while(counter > 0 and has_past): # keep going back until it reaches the desired version
                    tail_page_index = (indirection // max_records)*(self.num_columns+5)
                    indirection = self.bufferpool.get_page(self.name, tail_page_index, False).read_val(indirection)
                    counter -= 1
                    if indirection == -1 or indirection < self.tps:
                        has_past = False
                if has_past:
                    tail_page_index = (indirection // max_records)*(self.num_columns+5)
                    data = self.bufferpool.get_page(self.name, tail_page_index+column_index+4, False).read_val(indirection)
                    total_sum += data
                else: # if it's asking for versions that doesn't exist
                    data = self.bufferpool.get_page(self.name, base_page_index+column_index+4, True).read_val(rid)
                    total_sum += data
        
        return total_sum

    def close(self):
        filename = "tabledata.pickle"
        path = os.path.join(self.path, filename)
        with open(path, 'wb') as f:
            pickle.dump(self, f) #dump all metadata, pagedirectory, and index 
    
    def open(self):
        filename = "tabledata.pickle"
        path = os.path.join(self.path, filename)
        with open(path, 'rb') as f:
            loaded_table = pickle.load(f)

        # directly update the current instance's attributes
        self.__dict__.update(loaded_table.__dict__)

        # Re-bind bufferpool's reference to this table
        self.bufferpool.add_table(self.name, self)
        
