from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

db = Database()
db.open("./IMDEAD")
grades_table = db.create_table('Grades', 5, 0)
q = Query(grades_table)

record1 = [45, 75, 82, 31, 49]
record2 = [47, 800, 11, 22, 99]
record3 = [52, 200, 11, 22, 99]
record4 = [45, 800, 11, 99, 99]
record5 = [47, 2, 3, 4, 5]

t = Transaction()
t.add_query(q.insert, grades_table, *record1)
t.add_query(q.insert, grades_table, *record2)
t.add_query(q.insert, grades_table, *record3)

t1 = Transaction()
t1.add_query(q.select, grades_table, 47, 0, [1, 1, 1, 1, 1])
t1.add_query(q.select, grades_table, 52, 0, [1, 1, 1, 1, 1])
t1.add_query(q.select, grades_table, 45, 0, [1, 1, 1, 1, 1])

t2 = Transaction()
t2.add_query(q.select, grades_table, 52, 0, [1, 1, 1, 1, 1])
t2.add_query(q.select, grades_table, 45, 0, [1, 1, 1, 1, 1])
t2.add_query(q.select, grades_table, 47, 0, [1, 1, 1, 1, 1])

t3 = Transaction()
t3.add_query(q.update, grades_table, 47, *record4)

t_worker = TransactionWorker()
t_worker.add_transaction(t)
t_worker.run()
t_worker.join()

x_worker = TransactionWorker() #select thread (S lock)
y_worker = TransactionWorker() #select thread (S lock)
z_worker = TransactionWorker() #update thread (X lock)

x_worker.add_transaction(t1)
y_worker.add_transaction(t2)
z_worker.add_transaction(t3)

x_worker.run()
y_worker.run()
z_worker.run()

x_worker.join()
y_worker.join()
z_worker.join()