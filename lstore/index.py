"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees.OOBTree import OOBTree


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices: list[OOBTree | None] = [None] * table.num_columns
        self.table = table

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if value in self.indices[column]:
            return self.indices[column][value]
        return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        rids = []
        tree = self.indices[column]
        for values in list(tree.values[begin:end]):
            rids.extend(values)
        return rids

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = OOBTree()

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

    def push_index(self, columns, rid):
        for column_number in range(self.table.num_columns):
            if column_number == self.table.key:
                continue
            if self.indices[column_number] is None:
                self.create_index(column_number)
            tree = self.indices[column_number]
            if columns[column_number] not in tree:
                tree[columns[column_number]] = [rid]
            else:
                tree[columns[column_number]].append(rid)
