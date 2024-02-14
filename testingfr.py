from page import Page
from table import Table
import struct
from query import Query

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
t.update_record(47, *record1)
print(struct.unpack('i', t.page_directory[5].tailPage_directory[0]["page"][0*64:0*64+struct.calcsize('i')])[0]) #tail record points back to base record
print(struct.unpack('i',t.page_directory[5].data[1*64:1*64+struct.calcsize('i')])[0]) #base record points to tail record
print(struct.unpack('i', t.page_directory[3].tailPage_directory[0]["page"][0*64:0*64+struct.calcsize('i')])[0])

record1 = [450, 5, 170, None, 190]
t.update_record(47, *record1)
print(struct.unpack('i', t.page_directory[1].tailPage_directory[0]["page"][1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.page_directory[3].tailPage_directory[0]["page"][1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.page_directory[3].tailPage_directory[0]["page"][1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.page_directory[5].tailPage_directory[0]["page"][0*64:0*64+struct.calcsize('i')])[0]) #tail record points back to base record
print(struct.unpack('i',t.page_directory[5].data[1*64:1*64+struct.calcsize('i')])[0])
print(struct.unpack('i', t.page_directory[5].tailPage_directory[0]["page"][1*64:1*64+struct.calcsize('i')])[0]) #tail record points back to base record

