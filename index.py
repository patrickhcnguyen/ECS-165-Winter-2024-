from BTrees._OOBTree import OOBTree
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
RID_COLUMN = 1
# BTREE_DEGREE = 5 #the number of values stored in one b-tree node <-- can experiment with different values later
class Index:

    def __init__(self, table):
        self.indices = [None] *  table.num_columns # Indices store the b-tree for each column that stores all the indexes for that specific column
        # self.indices[RID_COLUMN] = BTree(BTREE_DEGREE) # Initializes indexes for key column
        self.table = table

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):
        # rid_value = self.indices[column].search(value) #finds
        rid_value = self.indices[column].values(value,value) 
        return rid_value # return the RID of value
    
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, column, begin, end):
        rid_list = self.indices[column].values(begin, end)
        return rid_list #returns list of RID's of all values that fall within the range

    """
    # Adds index to a column (should be called when a new row/record is inserted)
    """
    def add_index(self, column, key, rid):
        b_tree = self.indices[column]
        b_tree.insert(key, rid)

    """
    # Updates RID of key from old_rid to new_rid
    """
    def update_index(self, column, key, old_rid, new_rid):
        b_tree = self.indices[column]
        b_tree[key] = new_rid

    """
    # Lazy deletes index of key and RID
    """
    def lazy_delete_index(self, column, key, rid):
        # b_tree = self.indices[column]
        # b_tree.lazy_delete(key, rid)
        pass
        
    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        # self.indices[column_number] = BTree(BTREE_DEGREE)
        self.indices[column_number] = OOBTree([])

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        self.indices[column_number] = None

"""
# B-Tree data structure that supports duplicate RID's for one key.
# main functions:
# search(key) - returns RID(s) 
# search_range(key) - returns all RID(s) in the range
# update(key, old_rid, new_rid) - updates old_rid of key to new_rid
# lazy_delete(key, rid) - sets rid of key to -1 if the value is deleted <-- ignore -1 rids for query operations
#   - don't have to use this function! it's just an idea for deletion
# Feel free to change anything for the BTree/BNode to work with query/or if there is any errors
"""
class BNode:
    def __init__(self, t, is_leaf):
        self.keys = [None] * (2 * t - 1)
        self.rids = [[] for _ in range(2 * t - 1)]
        self.t = t
        self.childrens = [None] * (2 * t)
        self.n = 0
        self.leaf = is_leaf
    
    def insert_not_full(self, new_key, new_rid):
        i = self.n - 1
        if self.leaf:
            while i >= 0 and self.keys[i] > new_key:
                self.keys[i + 1] = self.keys[i]
                self.rids[i + 1] = self.rids[i]
                i -= 1
            self.keys[i + 1] = new_key
            self.rids[i + 1] = [new_rid]
            self.n += 1

        else:
            while i >= 0 and self.keys[i] > new_key:
                i -= 1
            if self.childrens[i + 1].n == 2 * self.t - 1:
                self.split_child(i + 1, self.childrens[i + 1])
                if self.keys[i + 1] < new_key:
                    i += 1
            self.childrens[i + 1].insert_not_full(new_key, new_rid)

    def split_child(self, index, y):
        z = BNode(y.t, y.leaf)
        z.n = self.t - 1
        for j in range(0, self.t - 1):
            z.keys[j] = y.keys[j + self.t]
            z.rids[j] = y.rids[j + self.t]
        if not y.leaf:
            for j in range(0, self.t):
                z.childrens[j] = y.childrens[j + self.t]
        y.n = self.t - 1
        for j in range(self.n - 1, index - 1, -1):
            self.childrens[j + 1] = self.childrens[j]
        self.childrens[index + 1] = z
        for j in range(self.n - 1, index, -1):
            self.keys[j + 1] = self.keys[j]
            self.rids[j + 1] = self.rids[j]
        self.keys[index] = y.keys[self.t - 1]
        self.rids[index] = y.rids[self.t - 1]
        self.n += 1

    def search_range(self, low, high, list):
        for i in range(0, self.n):
            if not self.leaf:
                self.childrens[i].search_range(low, high, list)
            if self.keys[i] >= low and self.keys[i] <= high:
                list.append(self.rids[i])
        if not self.leaf:
            self.childrens[i + 1].search_range(low, high, list)

    def search(self, key):
        i = 0;
        while i < self.n and key > self.keys[i]:
            i += 1
            if self.keys[i] == None:
                break
        if i < self.n and self.keys[i] == key:
            return self.rids[i]
        if self.leaf:
            return None
        return self.childrens[i].search(key)
    
    def get_node(self, key):
        i = 0;
        while i < self.n and key > self.keys[i]:
            i += 1
        if self.keys[i] == key:
            return i, self
        if self.leaf:
            return None
        return self.childrens[i].get_node(key)

class BTree:
    def __init__(self, degree):
        self.root = None;
        self.t = degree;
    
    def update(self, key, old_rid, new_rid):
        if self.root and self.root.search(key):
            i, node = self.root.get_node(key)
            index = node.rids[i].index(old_rid)
            node.rids[i][index] = new_rid
        pass

    def search(self, key):
        if self.root:
            return self.root.search(key)
        else:
            return None
        
    def search_range(self, low, high):
        list = []
        if self.root:
            self.root.search_range(low, high, list)
        list = sum(list, [])
        return list

    def insert(self, key, rid):
        if self.search(key):
            i, node = self.root.get_node(key)
            node.rids[i].append(rid)     
        elif self.root is None:
            self.root = BNode(self.t, True)
            self.root.keys[0] = key
            self.root.rids[0] = [rid]
            self.root.n = 1

        else:
            if self.root.n == 2 * self.t - 1:
                s = BNode(self.t, False)
                s.childrens[0] = self.root
                s.split_child(0, self.root);
                i = 0;
                if s.keys[0] < key:
                    i += 1
                s.childrens[i].insert_not_full(key, rid)
                self.root = s
            else:
                self.root.insert_not_full(key, rid)

    def lazy_delete(self, key, rid):
        self.update(key, rid, -1) # sets old rid of key to -1 (lazy deletion)
