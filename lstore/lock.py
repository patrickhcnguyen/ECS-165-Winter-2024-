import threading

class Lock:
    def __init__(self, data_rid):
        self.read_count = 0
        self.write_count = 0
        self.rid = data_rid
        self.shared_transactions = [] # need to figure our how to identify transactions
        self.exclusive_transaction = []

    # read-lock: other transactions can only read but not write
    def get_shared_lock(self, transaction): #read lock
        if self.write_count == 0:
            self.shared_transactions.append(transaction)
            self.read_count += 1
            return True
        return False

    # write-lock: other transactions cannot read or write
    def get_exclusive_lock(self, transaction):
        if self.read_count == 0 and self.write_count == 0:
            self.exclusive_transaction.append(transaction)
            self.write_count += 1
            return True
        return False

    def release_shared_lock(self, transaction):
        if transaction in self.shared_transactions:
            self.shared_transactions.remove(transaction)
            self.read_count -= 1
            return True
        return False

    def release_exclusive_lock(self, transaction):
        if transaction in self.exclusive_transaction:
            self.exclusive_transaction.remove(transaction)
            self.write_count -= 1
            return True
        return False