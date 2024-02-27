from lstore.db import Database
from lstore.query import Query

db = Database()
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
for i in range(5):
    print(grades_table.page_directory[4].read_val(i), " ", grades_table.page_directory[5].read_val(i), " ", grades_table.page_directory[6].read_val(i), " ", grades_table.page_directory[7].read_val(i), " ", grades_table.page_directory[8].read_val(i))



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
q.update(45, *update2)
q.update(45, *update3)
q.update(45, *update4)
for i in range(64*2):
    q.update(45, *update4)
update5 = [None, None, 1, 2, 0]
for i in range(64*2): #64-6 = 58
    q.update(1, *update5)
q.update(45, *update2)

print("\nchecking base page for merge- ")
for i in range(5):
    print(grades_table.page_directory[4].read_val(i), " ", grades_table.page_directory[5].read_val(i), " ", grades_table.page_directory[6].read_val(i), " ", grades_table.page_directory[7].read_val(i), " ", grades_table.page_directory[8].read_val(i))