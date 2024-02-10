import struct

class Page: #This class manages a single base page and ALL tail pages that correspond to that single base page

    def __init__(self):
        self.num_records = 0
        self.max_records = 5 #page size is 512records*8= 4096 bytes for now, but we can experiement which will maximize read and merge performance
        self.data = bytearray(self.max_records*8) #holds a page-size worth of memory to dedicate for base data; our base page
        self.tailPage_directory = {}
        self.num_tails = -1 #stores the amount of tail pages minus 1
        self.create_new_tail()

    
    def has_capacity(self): #returns the amount of ints that can be added to the base page before the total capacity of 4096 bits is reached
        return (self.max_records-self.num_records)

    def write(self, value): #returns -1 if the base page is full and consequently, no change was done; returns rid (the index in the bytearray) if the value was written
        rid = -1
        if (self.has_capacity()>0):
            rid = self.num_records
            packed_bytes = struct.pack('i', value)
            self.data[rid*8:rid*8+len(packed_bytes)] = packed_bytes
            self.num_records += 1
        return rid
    

    def create_new_tail(self): #allocate an additional page-size worth of memory to dedicate to update data/tail data; our implementation of tail page
        self.num_tails += 1
        tail_dict = {"page": bytearray(self.max_records*8), "num_records": 0}
        self.tailPage_directory[self.num_tails] = tail_dict
        print("created new tail")

    def tail_has_capacity(self): #returns the amount of ints that can be added to the last tail page before the max_records capacity is reached
        return (self.max_records-self.tailPage_directory[self.num_tails]["num_records"])
    
    def write_update(self, value): #returns -1 if there was an error; returns rid of tail record if the value was written
        rid = -1
        if (self.tail_has_capacity()==0):
            self.create_new_tail()
        rid = self.tailPage_directory[self.num_tails]["num_records"]
        packed_bytes = struct.pack('i', value)
        self.tailPage_directory[self.num_tails]["page"][rid*8:rid*8+len(packed_bytes)] = packed_bytes
        self.tailPage_directory[self.num_tails]["num_records"] += 1
        rid = rid + (self.num_tails)*(self.max_records) #The value of the rid that will be returned is the index if all the tail pages were actually one continuous array
        return rid 
    
    """
    Based on our implementation of tail rid, in the final 2 lines of write_update, whenever a tail rid gets passed 
    to this page class, we must do the following operations to correctly locate the data the rid is referencing:
    tailPage = self.tailPage_directory[rid//self.max_records]["page"]
    data = tailPage[rid%self.max_records]
    """