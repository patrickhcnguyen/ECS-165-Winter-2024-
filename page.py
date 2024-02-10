
class Page: #This class manages a single base page and ALL tail pages that correspond to that single base page

    def __init__(self):
        self.num_records = 0
        self.max_records = 512 #4096/8
        self.data = bytearray(self.max_records*8) #page size is 4096 bytes for now, but we can experiement which will maximize read and merge performance
        self.tailPage_directory = {}
        self.num_tails = 0

    
    def has_capacity(self): #returns the amount of ints that can be added before the total capacity of 4096 bits is reached
        return (self.max_records-self.num_records)

    def write(self, value): #returns false if the page is full and did not change; returns true if the value was written
        progress = True
        if (self.has_capacity()>0):
            self.num_records += 1
            self.data.append(value)
        else:
            progress = False
        return progress
    
    def tail_has_capacity(self): #returns the amount of ints that can be added to the last tail page before the max_records capacity is reached
        return (self.max_records-self.num_records)

    def create_new_tail(self):
        self.num_tails += 1
        self.tailPage_directory.append(key: self.num_tails, page: bytearray(self.max_records*8), num_records: 0)

    def write_update(self, value):
        pass