from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

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
number_of_transactions = 100
number_of_operations_per_record = 1
num_threads = 8

keys = []
records = {}
seed(3562901)
print(grades_table.bufferpool)
key = 92107369
rid = grades_table.index.locate(grades_table.key, key)[0]
base_page_index = (rid//64)*(grades_table.num_columns+4)
columns = []
for i in range(grades_table.num_columns):
    print(base_page_index+i+4)
    print(grades_table.bufferpool.pool.keys())
    data = grades_table.bufferpool.get_page(grades_table.name, base_page_index+i+4, True).read_val(rid)
    print(grades_table.page_directory[base_page_index+i+4])
    columns.append(data)
        #result2 = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0].columns
print(rid, '        check this', key, ':', columns)
print(grades_table.page_directory[base_page_index])

"""
number_of_aggregates = 100
valid_sums = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum_version(keys[r[0]], keys[r[1]], 0, -1)
    if column_sum == result:
        valid_sums += 1
print("Aggregate version -1 finished. Valid Aggregations: ", valid_sums, '/', number_of_aggregates)

v2_valid_sums = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum_version(keys[r[0]], keys[r[1]], 0, -2)
    if column_sum == result:
        v2_valid_sums += 1
print("Aggregate version -2 finished. Valid Aggregations: ", v2_valid_sums, '/', number_of_aggregates)
if valid_sums != v2_valid_sums:
    print('Failure: Version -1 and Version -2 aggregation scores must be same.')

valid_sums = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: updated_records[x][0] if x in updated_records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum_version(keys[r[0]], keys[r[1]], 0, 0)
    if column_sum == result:
        valid_sums += 1
print("Aggregate version 0 finished. Valid Aggregations: ", valid_sums, '/', number_of_aggregates)
"""
db.close()