from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        self.result = 0
        self.transactions = []
        if (transactions != None):
            self.transactions = transactions
        self.thread = None
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        print("run thread")
        self.thread = threading.Thread(target=self.__run, args=())
        self.thread.start()
        # here you need to create a thread and call __run
    

    """
    Waits for the worker to finish
    """
    def join(self):
        self.thread.join()
        print("end thread")


    def __run(self):
        while len(self.transactions)!=0:
            aborted_transactions = []
            for transaction in self.transactions:
                # each transaction returns True if committed or False if aborted
                has_committed = transaction.run()
                self.stats.append(has_committed)
                if has_committed == False:
                    aborted_transactions.append(transaction)
            # stores the number of transactions that committed
            self.result = len(list(filter(lambda x: x, self.stats)))
            self.transactions = aborted_transactions