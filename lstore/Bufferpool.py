from datetime import datetime
from lstore.lru import LRU
from lstore.page import Page
import os
from pathlib import Path
import pickle


#Can be accessed from table class and vice versa
class BufferPool:
    def __init__(self, path='none', capacity=1000):
        self.parent_path = path          # path where pickle metadata can be saved.
        #self.LRU = LRU()         
        self.capacity = capacity  
        self.pool = {}           # Dictionary to store buffer pages indexed by buffer_id
        self.disk_page_count = 0
        self.table_access = {}

    def add_table(self, name, table):
        #print("Access to table ", name, " granted to bf")
        self.table_access[name] = table

    def initialPath(self, path):
        # Set the initial storage path for buffer pages
        self.path = path

    def initialTPS(self, t_name):
        # Initialize a table's transaction processing state if not already done
        if t_name not in self.tps.keys():
            self.tps[t_name] = {}

    def bufferPageChecker(self, buffer_id):
        # Check if a buffer page with the given buffer_id is in the pool
        return buffer_id in self.pool.keys()

    def addPages(self, t_name, page, page_key, is_base=True): #should be called from table class
        # Add a new page to the buffer pool and mark it as dirty
        #if is_base == True:
            #print("adding a base page to pool ", page_key)
        #else:
            #print("adding a tail page to pool ", page_key)
        if self.capacity == 0:
            self.evict_bufferpool()
        buffer_id = self.disk_page_count
        #print("adding a buffer id to pool ", buffer_id)
        self.pool[buffer_id] = [t_name, page, page_key, is_base]
        self.capacity-=1
        self.disk_page_count+=1
        #print(" pages currently in bufferpool: ", list(self.pool.keys()))
    
    def evict_bufferpool(self):
        page_to_evict = list(self.pool.keys())[0]
        oldest_time = self.pool[page_to_evict][1].timestamp
        for key in self.pool.keys():
            page = self.pool[key][1]
            if (page.pin==0): #select the page that is oldest of all the pages with 0 pins
                if (page.timestamp<oldest_time):
                    page_to_evict = key
                    oldest_time = page.timestamp
        #print("removing buffer page: ", page_to_evict)
        self.write_to_disk(page_to_evict)
        return
    
    def get_page_access(self, t_name, page_key, is_base=True):
        for key in self.pool.keys():
            if (self.pool[key][0]==t_name and self.pool[key][2]==page_key and self.pool[key][3]==is_base):
                return [self.pool[key][1], key]
        #  load page into bufferpool from disk if it's not currently in bufferpool
        [page, buffer_id] = self.load_from_disk(t_name, page_key, is_base)
        return [page, buffer_id]
    
    def load_from_disk(self, t_name, page_key, is_base=True): #for a single page
        table = self.table_access[t_name]
        if self.capacity==0:
            self.evict_bufferpool()
            self.capacity+=1
        path = ''
        if is_base == True:
            path = table.page_directory[page_key]
        else: 
            path = table.tail_page_directory[page_key]
        f = open(path, "r")
        page = Page()
        self.capacity -= 1
        lines = f.readlines()
        page.tps = int(lines[0])
        for i in range(len(lines)-1):
            page.write(int(lines[i+1]))
        self.addPages(t_name, page, page_key, is_base)
        return [page, self.disk_page_count]

    def write_to_disk(self, page_to_evict): #for a single page
        buffer_id = page_to_evict
        t_name = self.pool[buffer_id][0]
        #print("evict page ", page_to_evict, " with table name ", t_name)
        table = self.table_access[t_name]
        page = self.pool[buffer_id][1]
        page_key = self.pool[buffer_id][2]
        if (page.is_dirty==0 and page.num_records!=0):
            del self.pool[buffer_id]
            self.capacity+=1
            return
        filename = "page"+str(self.disk_page_count)
        path = os.path.join(table.base_path, filename)
        #print(" writing page ", filename)
        if self.pool[buffer_id][3] == False:
            path = os.path.join(table.tail_path, filename)
        f = open(path, "w")
        f.write(str(page.tps)+"\n")
        for i in range(page.num_records):
            data = page.read_val(i)
            f.write(str(data)+"\n")
        f.close()
        if self.pool[buffer_id][3] == True:
            table.page_directory[page_key] = path
            #print("path to evicted page: ", table.page_directory[page_key])
        else:
            table.tail_page_directory[page_key] = path
            #print("path to evicted page: ", table.tail_page_directory[page_key])
        del self.pool[buffer_id]
        self.capacity+=1

    def updatePool(self, buffer_id, page):
        # Update an existing page in the buffer pool and mark it as dirty
        self.pool[buffer_id] = page
        
    def get_page(self, t_name, page_key, is_base=True):
        page = self.get_page_access(t_name, page_key, is_base)[0]
        return page
    
    def get_page_copy(self, t_name, page_key, is_base=True):
        for key in self.pool.keys():
            if (self.pool[key][0]==t_name and self.pool[key][2]==page_key and self.pool[key][3]==is_base):
                return self.pool[key][1]
        #  load page from disk if it's not currently in bufferpool
        table = self.table_access[t_name]
        path = ''
        if is_base == True:
            path = table.page_directory[page_key]
        else: 
            path = table.tail_page_directory[page_key]
        f = open(path, "r")
        page = Page()
        lines = f.readlines()
        page.tps = int(lines[0])
        for i in range(len(lines)-1):
            page.write(int(lines[i+1]))
        return page

    def get_tail_pages(self, table_name):
        tail_pages = {}
        table = self.table_access[table_name]
        tail_page_start = (table.tps//table.max_records)*(table.num_columns+5)
        for page_key in range(tail_page_start, table.num_tail_pages+1):
            tail_pages[page_key] = self.get_page_copy(table_name, page_key, is_base=False)
        return tail_pages

    def replace_page(self, table_name, page_key, page):
        table = self.table_access[table_name]
        for key in self.pool.keys():
            if (self.pool[key][0]==table_name and self.pool[key][2]==page_key and self.pool[key][3]==True):
                page.is_dirty = 1
                self.pool[key][1] = page
        if page_key in table.page_directory:
            path = table.page_directory[page_key]
            f = open(path, 'w').close() #erases the current contents of the file
            f = open(path, 'w')
            f.write(str(page.tps)+"\n")
            for i in range(page.num_records):
                data = page.read_val(i)
                f.write(str(data)+"\n")
            f.close()
        pass



    def close(self):
        numkeys=0
        for key in self.pool.keys():
            numkeys+=1
        for i in range(numkeys):
            self.evict_bufferpool()
            self.disk_page_count+=1
        self.pool.clear()
        filename = "bufferpool.pickle"
        path = os.path.join(self.parent_path, filename)
        with open(path, 'wb') as f:
            pickle.dump(self, f) #dump all metadata, pagedirectory, and index
        
    def open(self):
        filename = "bufferpool.pickle"
        path = os.path.join(self.parent_path, filename)
        bp = BufferPool(self.parent_path)
        with open(path, 'rb') as f:
            bp = pickle.load(f)   
        capacity = bp.capacity
        self.capacity = capacity  
        disk_page_count = bp.disk_page_count
        self.disk_page_count = disk_page_count