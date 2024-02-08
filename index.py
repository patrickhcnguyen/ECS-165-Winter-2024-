#import b-tree library (might need to write our own b-tree class for other milestones or considering another library)
from BTrees._OOBTree import OOBTree
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

RID_COLUMN = 1

class Index:

    def __init__(self, table):
        # Index to b-tree for each column, all initialized to none
        self.indices = [None] *  table.num_columns
        # Create indices for key column
        self.create_index(table.key)
        self.table = table

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        rid_value = self.indices[column].values(value,value) #finds 
        
        return rid_value # return the RID value
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        rid_list = index_btree = self.indices[column]
        return rid_list #returns list of RID values

    """
    # optional: Create index on specific column <-- only works for key column right now
    """

    def create_index(self, column_number): # only for overall logic: need to test specifics with how to actually loop through page records
        index_btree = OOBTree([])
        pages = self.page_directory.get(column_number)
        rid_pages = self.page_directory.get(RID_COLUMN)

        for page in pages: #combine all pages of the column together
            combined_pages += page
            total_records += page.num_records
        for rid_page in rid_pages: #combine all pages of the rid column together
            combined_rid_pages += rid_page

        # assume total_records are the same for the column and rid column
        for i in range(0, total_records - 1):
            index_btree.insert(combined_pages[i], combined_rid_pages[i]) # insert [key, rid] into b-tree

    """
    # optional: Drop index of specific column <-- does not work right now
    """
    def drop_index(self, column_number):
        self.indices[column_number] = None
