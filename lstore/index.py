class Index:
    def __init__(self, table):
        # Correctly initialize an empty dictionary for each column's index
        self.table = table  # Reference to the table
        self.indices = [None] *  table.num_columns

    def locate(self, column, value):
        # Return RIDs for records matching value in column
        if self.indices[column] is None:
            # temporarily create an index and delete after getting values
            self.create_index(column)
            key_list = list(self.indices[column].get(value, []))
            self.drop_index(column)
            return key_list
        else:
            return list(self.indices[column].get(value, []))

    def locate_range(self, column, begin, end):
        # Return RIDs for records within range [begin, end] in column
        if self.indices[column] is None:
            raise ValueError(f"No index found for column {column}.")
        return [rid for key, rids in self.indices[column].items() if key is not None and (begin is None or begin <= key) and (end is None or key <= end) for rid in rids]

    def remove_index(self, column, value, rid):
        if rid not in self.indices[column][value]:
            return
        self.indices[column][value].remove(rid)

    def add_index(self, column, key, rid):
        # Ensure the column has an index before adding
        self.create_index(column)
        # Add index entry for a column
        if key not in self.indices[column]:
            self.indices[column][key] = set()
        self.indices[column][key].add(rid)

    def update_index(self, column, key, old_rid, new_rid):
        # Ensure the column has an index before updating
        if self.indices[column] is None:
            return 
        # Update index from old_rid to new_rid for a key in a column
        if key in self.indices[column]:
            self.indices[column][key].discard(old_rid)
            self.indices[column][key].add(new_rid)

    def delete_index(self, column, key, rid):
        # Ensure the column has an index before deleting
        if self.indices[column] is None:
            return  
        # Delete an index entry for a key and rid in a column
        if key in self.indices[column]:
            self.indices[column][key].discard(rid)
            if not self.indices[column][key]:  # If set is empty after deletion
                del self.indices[column][key]

    def create_index(self, column_number):
        # Create an index for a specific column by scanning all records
        if self.indices[column_number] is None:
            self.indices[column_number] = {}
            num_records = self.table.rid
            max_records = self.table.max_records #max_records per page
            for i in range (num_records//max_records):
                page = self.table.bufferpool.get_page_copy(self.table.name, i*(4+self.table.num_columns)+(4+column_number)).copy()
                for j in range(page.num_records):
                    self.add_index(column_number, page.read_val(j), i*(4+self.table.num_columns)+j)
            if (num_records%max_records!=0):
                i = num_records//max_records
                page = self.table.bufferpool.get_page_copy(self.table.name, i*(4+self.table.num_columns)+(4+column_number)).copy()
                for j in range(page.num_records):
                    self.add_index(column_number, page.read_val(j), i*(4+self.table.num_columns)+j)
            return



    def drop_index(self, column_number):
        # Drop an index for a specific column
        if self.indices[column_number] is not None:
            self.indices[column_number] = None