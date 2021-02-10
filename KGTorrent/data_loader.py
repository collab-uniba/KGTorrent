"""
This module defines the class that handles the data loading.
"""

import pandas as pd

# Imports for testing
from KGTorrent import config

class DataLoader:
    """
    This class stores the MetaKaggle version tables and the foreign key constraints table.
    """

    def __init__(self, constraints_file_path, meta_kaggle_path):
        """
        The constructor of this class loads Meta Kaggle and constraints ``.csv`` files from the given paths.

        Args:
            constraints_file_path: the path to the ``.csv`` file containing information on the foreign key constraints to be set. By default, it is located at ``/data/fk_constraints_data.csv``.
            meta_kaggle_path: The path to the folder containing the 29 ``.csv`` of the MetaKaggle tables.
        """

        # Dataframe containing constraints info:
        # (Referencing Table, Foreign Key, Referenced Table, Referenced Column, IsSolved)
        print('## Loading MetaKaggle constraints data...')
        self.constraints_df = pd.read_csv(constraints_file_path)

        # Array of table file names
        table = self.constraints_df['Table']
        referenced_table = self.constraints_df['Referenced Table']
        union = table.append(referenced_table, ignore_index=True)
        table_file_names = union.unique()

        # Dictionary of tables
        self.tables_dict = {}

        # Reading tables
        print('## Loading MeataKaggle csv tables from provided path...')
        for file_name in table_file_names:
            self.tables_dict[file_name] = pd.read_csv(meta_kaggle_path + '\\' + file_name)
            print(f'- {file_name} loaded.')

    def get_constraints_df(self):
        """
        This method returns the foreign key constraints ``pandas.DataFrame`` which contains constraints information:
        Referencing Table, Foreign Key, Referenced Table, Referenced Column.

        Returns:
            constraints_df: The ``pandas.DataFrame`` containing the foreign key constrains information.
        """
        return self.constraints_df

    def get_tables_dict(self):
        """
        This method returns the dictionary of all 29 MetaKaggle ``pandas.DataFrame`` tables.

        Returns:
            tables_dict: The dictionary whose keys are the table names and whose values​are the ``pandas.DataFrame`` tables
        """
        return self.tables_dict


if __name__ == '__main__':

    print("********************")
    print("*** LOADING DATA ***")
    print("********************")
    dataloader = DataLoader(config.constraints_file_path, config.meta_kaggle_path)

    print('CONSTRAINT DF\n', dataloader.get_constraints_df())
    print('TABLES\n', dataloader.get_tables_dict().keys())
    print(dataloader.get_tables_dict())
