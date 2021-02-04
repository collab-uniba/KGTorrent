
import pandas as pd

# Imports for testing
from KGTorrent import config

class DataLoader:

    def __init__(self, constraints_file_path, meta_kaggle_path):

        # Dataframe containing constraints info:
        # (Referencing Table, Foreign Key, Referenced Table, Referenced Column, IsSolved)
        print('## Loading MeataKaggle constraints data...')
        self.constraints_df = pd.read_csv(constraints_file_path)
        self.constraints_df['IsSolved'] = False

        #Array of table file names
        table = self.constraints_df['Table']
        referenced_table = self.constraints_df['Referenced Table']
        union = table.append(referenced_table, ignore_index=True)
        table_file_names = union.unique()

        #Dictionary of tables
        self.tables_dict = {}

        #Reading tables
        print('## Loading MeataKaggle csv tables from provided path...')
        for file_name in table_file_names:
            self.tables_dict[file_name] = pd.read_csv(meta_kaggle_path + '\\' + file_name)
            print(f'- {file_name} loaded.')

    def get_constraints_df(self):
        return self.constraints_df

    def get_tables_dict(self):
        return self.tables_dict


if __name__ == '__main__':

    print("********************")
    print("*** LOADING DATA ***")
    print("********************")
    dataloader = DataLoader(config.constraints_file_path, config.meta_kaggle_path)

    print('CONSTRAINT DF\n', dataloader.get_constraints_df())
    print('TABLES\n', dataloader.get_tables_dict().keys())
    print(dataloader.get_tables_dict())
