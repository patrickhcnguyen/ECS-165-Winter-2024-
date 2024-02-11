from page import Page
from table import Table
import struct

print("howdy")
p = Page()
print(p.max_records)
for x in range(7):
    rid = p.write(4)
    print(rid)
for x in range(14):
    rid = p.write_update(4)
    print(rid)

t = Table("sup", 1, 0)
p1 = t.page_directory[1] #p1 represents column 1 base page 1
p2 = t.page_directory[2] #p2 represents column 2 base page 1
print("\nwriting to base page of page 1")
rid = p1.write(55)
print(rid)
rid = p1.write(21)
print(rid)
rid = p1.write(23)
print(rid)
for x in range(4):
    rid = p1.write(5)
    print(rid) #if -1 prints, base page is full and no data was written
print("\nwriting to tail page of page 2")
for x in range(5):
    rid = p2.write_update(1)
    print(rid)
rid = p2.write_update(78)
rid = p2.write_update(109)
print("\nwriting to tail page of page 1")
for x in range(4):
    rid = p1.write_update(2)
    print(rid)
print("the data at index/rid 2 of column 1 base page is: ")
print(struct.unpack('i',p1.data[2*8:2*8+struct.calcsize('i')])[0])
print("the data at index/rid 5 of column 2 tail page is: ")
print(struct.unpack('i',p2.tailPage_directory[1]["page"][0*8:0*8+struct.calcsize('i')])[0])
print("the data at index/rid 6 of column 2 tail page is: ")
print(struct.unpack('i',p2.tailPage_directory[1]["page"][1*8:1*8+struct.calcsize('i')])[0])
print("the data at index/rid 1 of column 2 tail page is: ")
print(struct.unpack('i',p1.tailPage_directory[0]["page"][1*8:1*8+struct.calcsize('i')])[0])

# tests for index
print("\nTESTS FOR INDEX")

t1 = Table("aye", 1, 0)
print("setup index for column 0")
t1.index.create_index(0)

print("adds values 10 with rid 1 to indices of column 0")
t1.index.add_index(0, 10, 1)
print("adds values 20 with rid 2 to indices of column 0")
t1.index.add_index(0, 20, 2)
print("adds values 30 with rid 3 to indices of column 0")
t1.index.add_index(0, 30, 3)
print("adds values 40 with rid 4 to indices of column 0")
t1.index.add_index(0, 40, 4)
print("adds values 50 with rid 5 to indices of column 0")
t1.index.add_index(0, 50, 5)

print("the RID of value 50 is:", t1.index.locate(0, 50))
print("all RID's of values from 5 to 25 is:", t1.index.locate_range(0, 5, 25))

print("update the rid of value 50 from 5 to 6")
t1.index.update_index(0, 50, 5, 6)

print("the RID of value 50 is now updated to :", t1.index.locate(0, 50))