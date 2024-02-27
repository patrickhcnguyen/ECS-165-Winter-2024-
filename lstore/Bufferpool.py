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

    # def read_page(self, path):

    # def write_page(self, page, buffer_id):

    # def close(self):
        
    # need to create implementations in