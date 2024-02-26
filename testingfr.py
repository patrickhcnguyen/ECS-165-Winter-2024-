from lstore.page import Page
from lstore.table import Table
import struct
from lstore.query import Query

print("howdy")
t = Table("howdy", 5, 0)
record1 = [45, 75, 82, 31, 49]
q = Query(t)
q.insert(*record1)
print(struct.unpack('i',t.page_directory[0].data[0*64:0*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[1].data[0*64:0*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[4].data[0*64:0*64+struct.calcsize('i')])[0])
record1 = [47, 800, 11, 22, 99]
print("let's seee")
t.insert_record(*record1)
print("test i")
print(struct.unpack('i',t.page_directory[0].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[1].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[2].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[3].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[4].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[5].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[6].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[7].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i',t.page_directory[8].data[1*64:1*64+struct.calcsize('i')])[0])

record1 = [400, 200, 150, 100, 19]
t.insert_record(*record1)

print("testing update")
record1 = [450, 130, 170, 380, 190]
q.update(47, *record1)
print(struct.unpack('i', t.tail_page_directory[0].data[0*64:0*64+struct.calcsize('i')])[0]) #tail record points back to base record
print(struct.unpack('i', t.tail_page_directory[1].data[0*64:0*64+struct.calcsize('i')])[0]) #base record points to tail record
print(struct.unpack('i', t.tail_page_directory[2].data[0*64:0*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[3].data[0*64:0*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[4].data[0*64:0*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[5].data[0*64:0*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[6].data[0*64:0*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[7].data[0*64:0*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[8].data[0*64:0*64+struct.calcsize('i')])[0])

print("2nd update on the same record")
record1 = [450, 5, 170, None, 190]
t.update_record(47, *record1)
print(struct.unpack('i', t.tail_page_directory[0].data[1*64:1*64+struct.calcsize('i')])[0]) #tail record points back to base record
print(struct.unpack('i', t.tail_page_directory[1].data[1*64:1*64+struct.calcsize('i')])[0]) #base record points to tail record
print(struct.unpack('i', t.tail_page_directory[2].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[3].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[4].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[5].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[6].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[7].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[8].data[1*64:1*64+struct.calcsize('i')])[0])
print("checking work ")
print(struct.unpack('i',t.page_directory[5].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.tail_page_directory[0].data[0*64:0*64+struct.calcsize('i')])[0]) #tail record points back to base record

print("hello")
t1 = Table("hello", 5, 0)
record1 = [45, 75, 80, 85, 90]
record2 = [47, 80, 90, 100, 105]

record3 = [47, 1, 2, 3, 4]
record7 = [47, 2, 3, 5, 6]
record8 = [47, 3, 4, 6, 9]
record11 = [45, 0, 1, 2, 3]
recordh = [45, 2, 2, 2, 2]

record4 = [49, 100, 110, 102, 103]
record5 = [90, 20, 130, 103, 101]
record6 = [3, 20, 1320, 1303, 1101]

t1.insert_record(*record1)
t1.insert_record(*record2) # version <= -3
# print(t1.select_record(45, 1, [1,1,1,1,1])[0].columns)
t1.update_record(47, *record3) # version -2
t1.update_record(47, *record7) # version -1
t1.update_record(47, *record8) # version 0
# print(t1.select_record_version(47, 1, [1,1,1,1,1], -4)[0].columns)
# t1.update_record(47, *record7)
# print(t1.select_record(47, 1, [1,1,1,1,1])[0].columns)

t1.update_record(45, *record11) # version 0
t1.insert_record(*record4)
t1.insert_record(*record5)
t1.insert_record(*record6)

print(t1.select_record_version(47, 1, [1,1,1,1,1], -100)[0].columns)
print(t1.select_record_version(45, 1, [1,1,1,1,1], -100)[0].columns)
t1.merge()
print(t1.select_record_version(47, 1, [1,1,1,1,1], -100)[0].columns)
print(t1.select_record_version(45, 1, [1,1,1,1,1], -100)[0].columns)

t1.update_record(45, *recordh)
t1.merge()
print(t1.select_record_version(45, 1, [1,1,1,1,1], -100)[0].columns)

q = Query(t1)
print("sum", q.sum_version(46, 49, 1, 0))
