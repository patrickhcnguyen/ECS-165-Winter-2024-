from datetime import datetime
from lstore.lru import LRU
from lstore.page import Page
import csv
from pathlib import Path

class BufferPool:
    def __init__(self, capacity=500):
        self.path = ""          # Path where buffer pages are stored.
        self.LRU = LRU()         
        self.capacity = capacity  
        self.pool = {}           # Dictionary to store buffer pages indexed by buffer_id

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

    def addPages(self, buffer_id, page):
        # Add a new page to the buffer pool and mark it as dirty
        self.pool[buffer_id] = page
        self.pool[buffer_id].set_dirty()

    def updatePool(self, buffer_id, page):
        # Update an existing page in the buffer pool and mark it as dirty
        self.pool[buffer_id] = page
        self.pool[buffer_id].set_dirty()

    def isFull(self):
        return self.LRU.isFull()

    def bufferid_path_csv(self, buffer_id):
        # Generate the file path for a buffer page based on buffer_id
        dirname = Path(self.path) / str(buffer_id[2]) / str(buffer_id[3]) / buffer_id[1]
        file_path = dirname / f"{buffer_id[4]}.csv"
        return file_path

    def get_page(self, buffer_id):
        # Get a buffer page based on buffer_id
        if buffer_id in self.pool:
            return self.pool[buffer_id]

        path = self.bufferid_path_csv(buffer_id)
        # Create a new page if it doesn't exist
        if not path.is_file():
            page = Page()
            self.addPages(buffer_id, page)
            return page
        # Return the existing page if it's already in the pool
        else:
            if not self.bufferPageChecker(buffer_id):
                self.pool[buffer_id] = self.read_page(path)
            self.LRU.put(buffer_id, datetime.timestamp(datetime.now()))
            return self.pool[buffer_id]


    # def read_page(self, path):

    # def write_page(self, page, buffer_id):

    # def close(self):
        
    # need to create implementations in