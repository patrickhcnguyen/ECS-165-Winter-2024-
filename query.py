from table import Table, Record
from index import Index
import struct

RID_COLUMN = 1

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        rid = self.index.locate(self.key, primary_key)
        if rid is None:
            return False  # if primary key is not found

        # marking the record as deleted by setting a special value like -1
        page_number = rid // self.max_records
        record_number = rid % self.max_records
        base_page = self.page_directory[page_number]
        
        # writing special deletion marker like -1 into record's first user column
        deletion_marker = struct.pack('i', -1)
        base_page.data[record_number * 64 + (4 * 8):record_number * 64 + (5 * 8)] = deletion_marker  # Assuming 4 system columns

        # Optionally, update indices to reflect deletion
        self.index.lazy_delete_index(self.key, primary_key, rid)

        return True

    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        self.table.insert_record(*columns)
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        rid = self.index.locate(self.key, primary_key)
        if rid is None:
            return False  # if primary key not found

        # creating new tail record containing updated columns

        rid_within_page = rid % self.max_records
        # updating the first column of the table, the indirection column 
        # tail record of indirection column points to prev version of data -> will be -1 if the prev version is the base record, due to our implementation of insert_record
        prev_version = struct.unpack('i', self.page_directory[rid // self.max_records].data[rid_within_page*64 : rid_within_page*64+struct.calcsize('i')])[0]
        tail_rid = self.page_directory[rid // self.max_records].write_update(prev_version)
        if tail_rid == -1:
            return False  # if failed to create a tail record
        

        # updating the actual data columns
        for i, column_value in enumerate(columns):
            if column_value is not None:
                self.page_directory[(rid // self.max_records) + i + 4].write_update(column_value)  # Skipping first 4 system columns
            else:
                prev_value = struct.unpack('i', self.page_directory[(rid // self.max_records) + i + 4].data[rid_within_page*64:rid_within_page*64+struct.calcsize('i')])[0]
                self.page_directory[(rid // self.max_records) + i + 4].write_update(prev_value)
            

        # updating the indirection column for the base record to point to the new tail record
        base_page = self.page_directory[rid // self.max_records]
        base_page.data[rid_within_page * 64: (rid_within_page * 64) + 64] = struct.pack('i', tail_rid)  

        self.index.update_index(self.key, primary_key, rid, tail_rid)
        return True


    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        total_sum = 0
        # iterating over range of primary keys
        for key in range(start_range, end_range + 1):
            # finding RID for current key using table's index
            rid = self.index.locate(self.key, key)
            if rid is not None:
                # calculating which page and where in page record is at 
                page_number = rid // self.max_records
                record_number = rid % self.max_records
                page = self.page_directory[page_number]
                value_bytes = page.data[record_number * 64 + (aggregate_column_index * 8): record_number * 64 + ((aggregate_column_index + 1) * 8)]
                value = struct.unpack('i', value_bytes)[0] # bytes to integer 
                total_sum += value # summing value
        return total_sum


    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

