from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

db = Database()
db.open("./MOCHI")
grades_table = db.create_table('Grades', 5, 0)
q = Query(grades_table)

record1 = [45, 75, 82, 31, 49]
record2 = [None, 800, 11, 22, 99]
record3 = [None, 200, 11, 22, 99]
record4 = [None, 800, 11, 99, 99]
record5 = [None, 2, 3, 4, 5]

insert1 = [11, 72, 94, 18, 1]
insert2 = [90, 182, 28, 364, 2]
insert3 = [283, 18, 2, 28, 19]
insert4 = [9, 263, 1, 28, 3]
insert5 = [27, 37, 48, 29, 3]
insert6 = [73, 2, 18, 282, 2]
insert7 = [6, 28, 37, 94, 261]

s = Transaction()
s.add_query(q.insert, grades_table, *record1)

t1 = Transaction()
t1.add_query(q.update, grades_table, 45, *record2)
t1.add_query(q.update, grades_table, 45, *record3)
t1.add_query(q.insert, grades_table, *insert7)

t2 = Transaction()
t2.add_query(q.update, grades_table, 45, *record4)
t2.add_query(q.update, grades_table, 45, *record5)
t2.add_query(q.sum_version, grades_table, 11, 90, 3, -1)
t2.add_query(q.insert, grades_table, *insert6)


t3 = Transaction()
t3.add_query(q.insert, grades_table, *insert1)
t3.add_query(q.sum_version, grades_table, 11, 90, 3, -1)

t4 = Transaction()
t4.add_query(q.update, grades_table, 45, *record1)
t4.add_query(q.update, grades_table, 45, *record3)

t5 = Transaction()
t5.add_query(q.insert, grades_table, *insert2)
t5.add_query(q.insert, grades_table, *insert3)
t5.add_query(q.insert, grades_table, *insert4)
t5.add_query(q.insert, grades_table, *insert5)


"""t2 = Transaction()
t2.add_query(q.select, grades_table, 52, 0, [1, 1, 1, 1, 1])
t2.add_query(q.select, grades_table, 45, 0, [1, 1, 1, 1, 1])
t2.add_query(q.select, grades_table, 47, 0, [1, 1, 1, 1, 1])

t3 = Transaction()
t3.add_query(q.update, grades_table, 47, *record4)"""

"""t_worker = TransactionWorker()
t_worker.add_transaction(t)
t_worker.run()
t_worker.join()"""

z_worker = TransactionWorker()
z_worker.add_transaction(s)
z_worker.run()
z_worker.join()

print("/////////////")

x_worker = TransactionWorker() #select thread (S lock)
y_worker = TransactionWorker() #select thread (S lock)
#z_worker = TransactionWorker() #update thread (X lock)

x_worker.add_transaction(t1)
x_worker.add_transaction(t5)
x_worker.add_transaction(t4)
y_worker.add_transaction(t2)
y_worker.add_transaction(t3)
#z_worker.add_transaction(t3)

x_worker.run()
y_worker.run()
#z_worker.run()

x_worker.join()
y_worker.join()
#z_worker.join()
db.close()