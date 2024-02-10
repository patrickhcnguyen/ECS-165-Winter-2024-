
class Page: #This class manages a single base page and ALL tail pages that correspond to that single base page

    def __init__(self):
        self.num_records = 0
        self.max_records = 512 #page size is 512records*8= 4096 bytes for now, but we can experiement which will maximize read and merge performance
        self.data = bytearray(self.max_records*8) #holds a page-size worth of memory to dedicate for base data; our base page
        self.tailPage_directory = {}
        self.num_tails = 0

    
    def has_capacity(self): #returns the amount of ints that can be added to the base page before the total capacity of 4096 bits is reached
        return (self.max_records-self.num_records)

    def write(self, value): #returns false if the base page is full and consequently, no change was done; returns true if the value was written
        progress = True
        if (self.has_capacity()>0):
            self.data[self.num_records*8] = value
            self.num_records += 1
        else:
            progress = False
        return progress
    

    def create_new_tail(self): #allocate an additional page-size worth of memory to dedicate to update data/tail data; our implementation of tail page
        self.num_tails += 1
        tail_dict = {"page": bytearray(self.max_records*8), "num_records": 0}
        self.tailPage_directory[self.num_tails] = tail_dict

    def tail_has_capacity(self): #returns the amount of ints that can be added to the last tail page before the max_records capacity is reached
        return (self.max_records-self.tailPage_directory[self.num_tails]["num_records"])
    
    def write_update(self, value):
        progress = True
        if (self.tail_has_capacity()==0):
            self.create_new_tail()
        num_records = self.tailPage_directory[self.num_tails]["num_records"]
        self.tailPage_directory[self.num_tails]["page"][num_records*8] = value
        self.tailPage_directory[self.num_tails]["num_records"] += 1
        return progress