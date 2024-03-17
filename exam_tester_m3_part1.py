from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from lstore.Bufferpool import BufferPool

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# creating grades table
grades_table = db.create_table('Grades', 5, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_transactions = 100
num_threads = 8

# create index on the non primary columns
try:
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

keys = []
records = {}
seed(3562901)

# array of insert transactions
insert_transactions = []

for i in range(number_of_transactions):
    insert_transactions.append(Transaction())

for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    t = insert_transactions[i % number_of_transactions]
    t.add_query(query.insert, grades_table, *records[key])

transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker())
    
for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(insert_transactions[i])



# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()


# Check inserted records using select query in the main thread outside workers
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record.columns, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")

bp = db.bufferpool
numkeys=0
for key in bp.pool.keys():
    numkeys+=1
for i in range(numkeys):
    bp.evict_bufferpool()
    bp.disk_page_count+=1

key = 92107369
rid = grades_table.index.locate(grades_table.key, key)[0]
base_page_index = (rid//64)*(grades_table.num_columns+4)
columns = []
for i in range(grades_table.num_columns):
    data = grades_table.bufferpool.get_page(grades_table.name, base_page_index+i+4, True).read_val(rid)
    print(grades_table.page_directory[base_page_index+i+4])
    columns.append(data)
        #result2 = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0].columns
print(rid, '        check this', key, ':', columns)
print(grades_table.page_directory[base_page_index])

db.close()