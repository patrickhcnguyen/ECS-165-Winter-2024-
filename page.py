
class Page: #It seems the table will be managing all the logical reasoning of the pages; this class simply creates a general page (not specified if it is base or tail) to hold data

    def __init__(self):
        self.num_records = 0
        self.max_records = 512 #4096/8
        self.data = bytearray(4096) #page size is 4096 bytes for now, but we can experiement which will maximize read and merge performance

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
# HOW DO WE ENSURE TWO VALUES AREN'T TRYING TO BE APPENDED AT ONCE messing up the capacity count?
# We can implement a special lock  over the page towards write operations, page should still be able to handle the other processes (update & read)
