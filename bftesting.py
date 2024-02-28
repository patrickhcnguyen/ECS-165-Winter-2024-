from lstore.db import Database
from lstore.query import Query

db = Database()
db.open("./ECS165A")
grades_table = db.create_table('Grades', 5, 0)
q = Query(grades_table)

print("\nwriting to base page- ")
record1 = [45, 75, 82, 31, 49]
record2 = [47, 800, 11, 22, 99]
record3 = [52, 200, 11, 22, 99]
record4 = [8, 800, 11, 99, 99]
record5 = [1, 2, 3, 4, 5]
q.insert(*record1)
q.insert(*record2)
q.insert(*record3)
q.insert(*record4)
q.insert(*record5)

print("\nwriting to tail page- ")
record1 = [45, 22, 33, 44, 7]
record2 = [None, 114, 5, None, None]
#record3 = [52, 200, 11, 22, 99]
record4 = [None, None, 1, None, None]
record5 = [None, None, None, None, 15]
q.update(45, *record1)
q.update(47, *record2)
#q.update(52, *record3)
q.update(8, *record4)
q.update(1, *record5)
update1 = [68, 4, 2, 3, 9]
update2 = [None, None, 33, 12, 7]
update3 = [None, 22, 33, 44, 7]
update4 = [None, 11, 33, 44, 7]
q.update(45, *update1)
r= q.select_version(45, 0, [1, 1, 1, 1, 1], 0)[0].columns
print(r)
print("\n CHECK HERE ")
q.update(45, *update2)
r= q.select_version(45, 0, [1, 1, 1, 1, 1], 0)[0].columns
#print(grades_table.tail_page_directory[4])
print(r)
q.update(45, *update3)
q.update(45, *update4)
for i in range(64*2):
    q.update(45, *update4)
update5 = [None, None, 1, 2, 0]
for i in range(64*2): #64-6 = 58
    q.update(1, *update5)
q.update(45, *update2)


r= q.select_version(45, 0, [1, 1, 1, 1, 1], 0)[0].columns
print(r)
r= q.select(47, 0, [1, 1, 1, 1, 1])[0].columns
print(r)
r= q.select(52, 0, [1, 1, 1, 1, 1])[0].columns
print(r)
r= q.select(8, 0, [1, 1, 1, 1, 1])[0].columns
print(r)
r= q.select(1, 0, [1, 1, 1, 1, 1])[0].columns
print(r)
#print(grades_table.page_directory)

#for stuff in grades_table.page_directory: # just to see where the base pages are
#    print(grades_table.page_directory[stuff])
