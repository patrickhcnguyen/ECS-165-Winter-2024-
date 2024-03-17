import struct
import datetime

class Page: #This class manages a physical page; the table class is in charge of differentiating base vs tail logic

    def __init__(self):
        self.num_records = 0
        self.max_records = 64 #page size is 64records*64bits_per_record= 4096 bytes for now, but we can experiement which will maximize read and merge performance
        self.data = bytearray(self.max_records*64) #holds a page-size worth of memory to dedicate for base data; our base page
        self.is_dirty = 0
        self.pin = 0
        self.tps = 0
        self.timestamp = datetime.datetime.now()
    
    def has_capacity(self): #returns the amount of ints that can be added to the base page before the total capacity of 4096 bits is reached
        return (self.max_records-self.num_records)

    def write(self, value, rid=None): #returns -1 if the base page is full and consequently, no change was done; returns rid (the index in the bytearray) if the value was written
        self.timestamp = datetime.datetime.now()

        index_within_page = -1
        if (self.has_capacity()>0):
            self.is_dirty = 1
            index_within_page = self.num_records
            if rid!=None:
                index_within_page = rid%self.max_records
            packed_bytes = struct.pack('i', value)
            self.data[index_within_page*64:index_within_page*64+len(packed_bytes)] = packed_bytes
            self.num_records += 1
        return index_within_page
    
    def overwrite(self, rid, value): #returns rid (the index in the bytearray) if the value was written
        self.timestamp = datetime.datetime.now()
        self.is_dirty = 1
        index_within_page = rid % self.max_records
        packed_bytes = struct.pack('i', value)
        self.data[index_within_page*64:index_within_page*64+len(packed_bytes)] = packed_bytes
        return
    
    def read_val(self, rid):
        self.timestamp = datetime.datetime.now()
        index_within_page = rid % self.max_records
        value = struct.unpack('i', self.data[index_within_page*64:index_within_page*64+struct.calcsize('i')])[0]
        return value
    
    # creates a shallow copy of the instance
    def copy(self):
        new_instance = Page()
        new_instance.num_records = self.num_records
        new_instance.max_records = self.max_records
        new_instance.data = self.data[:]
        return new_instance