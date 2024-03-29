from lstore.table import SCHEMA_ENCODING_COLUMN, Table, Record
from lstore.index import Index
import struct
RID_COLUMN = 1
class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
class Query:
    def __init__(self, table):
        self.table = table

    def delete(self, primary_key):
        # Locate the RID for the primary key
        result = self.table.index.locate(self.table.key, primary_key)
        if not result:
            return False  # if primary key is not found
        rid = result[0]

        # Calculate the page and record number
        max_records = self.table.max_records
        page_number = rid // max_records
        record_number = rid % max_records

        # Determine the base page index for the schema encoding column
        schema_encoding_page_index = self.table.num_columns + SCHEMA_ENCODING_COLUMN

        # Retrieve the base page for the schema encoding column
        base_page = self.table.bufferpool.get_page(self.table.name, page_number * (self.table.num_columns + 4) + schema_encoding_page_index, True)

        # Mark the record as deleted by setting its schema encoding to -1
        base_page.overwrite(record_number, -1)

        # Update indices to reflect deletion
        self.table.index.remove_index(self.table.key, primary_key, rid)

        return True


    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        primary_key = columns[self.table.key]
        rid = self.table.index.locate(self.table.key, primary_key)
        if rid == []:
            self.table.insert_record(*columns)  # if primary key not found
            return True
        else:
            return False

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
        return self.table.select_record(search_key, search_key_index, projected_columns_index)
    
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
        return self.table.select_record_version(search_key, search_key_index, projected_columns_index, relative_version)
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        rid_list = self.table.index.locate(self.table.key, primary_key)
        if rid_list != []:
            self.table.update_record(primary_key, *columns)
            return True
        else:
            return False  # if primary key not found



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        result = self.table.sum_records(start_range, end_range, aggregate_column_index)
        if result:
            return result
        else:
            return False
    
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
        result = self.table.sum_records_version(start_range, end_range, aggregate_column_index, relative_version)
        if result:
            return result
        else:
            return False
    
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
