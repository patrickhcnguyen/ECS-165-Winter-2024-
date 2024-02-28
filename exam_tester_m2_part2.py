from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 1

seed(3562901)
for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

# Simulate updates
updated_records = {}
keys = sorted(list(records.keys()))
for _ in range(number_of_updates):
    for key in keys:
        updated_records[key] = records[key].copy()
        for j in range(2, grades_table.num_columns):
            value = randint(0, 20)
            updated_records[key][j] = value
keys = sorted(list(records.keys()))


for key in keys:
    record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != updated_records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
print("Select for version 0 finished")



db.close()