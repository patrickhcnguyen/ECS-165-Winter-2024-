from db import Database
from query import Query
import struct
from random import choice, randint, sample, seed

db = Database()
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
seed(3562901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    #skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    print('inserted', records[key])
    max_records=64
    page_set = i//max_records
    rid=i%max_records
    print("Checking updates: ",struct.unpack('i', grades_table.page_directory[4+page_set*(5+4)].data[rid*64:rid*64+struct.calcsize('i')])[0])
print("Insert finished")

updated_records = {}
k=-1
for key in records:
    k+=1
    updated_columns = [None, None, None, None, None]
    updated_records[key] = records[key].copy()
    for i in range(2, grades_table.num_columns):
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        # update our test directory
        updated_records[key][i] = value
    query.update(key, *updated_columns)
    max_records=64
    page_set = k//max_records
    rid=k%max_records
    print("Checking updates: ",struct.unpack('i', grades_table.page_directory[4+page_set*(5+4)].tailPage_directory[0]["page"][rid*64:rid*64+struct.calcsize('i')])[0])
    print("Checking updates: ",struct.unpack('i', grades_table.page_directory[5+page_set*(5+4)].tailPage_directory[0]["page"][rid*64:rid*64+struct.calcsize('i')])[0])
    print("Checking updates: ",struct.unpack('i', grades_table.page_directory[6+page_set*(5+4)].tailPage_directory[0]["page"][rid*64:rid*64+struct.calcsize('i')])[0])
    print("Checking updates: ",struct.unpack('i', grades_table.page_directory[7+page_set*(5+4)].tailPage_directory[0]["page"][rid*64:rid*64+struct.calcsize('i')])[0])
    print("Checking updates: ",struct.unpack('i', grades_table.page_directory[8+page_set*(5+4)].tailPage_directory[0]["page"][rid*64:rid*64+struct.calcsize('i')])[0])

    
    #check version 0 for record
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != updated_records[key][j]:
            error = True
    if error:
        print('update error on', records[key], 'and', updated_columns, ':', record, ', correct:', updated_records[key])

