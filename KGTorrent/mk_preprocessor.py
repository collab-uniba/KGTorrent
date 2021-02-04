import pandas as pd
import numpy as np

# Imports for testing
from KGTorrent import config
from KGTorrent.data_loader import DataLoader

class MkPreprocessor:

    def __init__(self, tables_dict, constraints_df):
        # Keep track tables that have been already visited during the current recursive call
        self.already_visited = []

        # List of table names that need to be processed

        # Dictionary of dataframes that need to be processed
        self.tables_dict = tables_dict

        # Dataframe containing constraints info:
        # (Referencing Table, Foreign Key, Referenced Table, Referenced Column, IsSolved)
        self.constraints_df = constraints_df

        # Dataframe containing info on row loss after referential integrity checks on referencing tables
        self.stats = pd.DataFrame(columns=['Table', 'Initial#rows', 'Final#rows'])

    def basic_preprocessing(self):

        for table_name in self.tables_dict.keys():
            # SPECIFIC TABLES FIX
            if 'ForumMessageVotes' in table_name:
                print(f'\t{table_name} fix indexing...')
                self.tables_dict[table_name].drop_duplicates(subset=['Id'], inplace=True)

            if 'Submissions' in table_name:
                print(f'\t{table_name} fix precision columns')

                self.tables_dict[table_name]['PublicScoreLeaderboardDisplay'] = self.tables_dict[table_name][
                    'PublicScoreLeaderboardDisplay'].map(lambda x: round(float(x), 3)).map(
                    lambda x: x if not np.isinf(x) else np.NaN)

                self.tables_dict[table_name]['PublicScoreFullPrecision'] = self.tables_dict[table_name][
                    'PublicScoreFullPrecision'].map(lambda x: round(float(x), 3)).map(
                    lambda x: x if not np.isinf(x) else np.NaN)

                self.tables_dict[table_name]['PrivateScoreLeaderboardDisplay'] = self.tables_dict[table_name][
                    'PrivateScoreLeaderboardDisplay'].map(lambda x: round(float(x), 3)).map(
                    lambda x: x if not np.isinf(x) else np.NaN)

                self.tables_dict[table_name]['PrivateScoreFullPrecision'] = self.tables_dict[table_name][
                    'PrivateScoreFullPrecision'].map(lambda x: round(float(x), 3)).map(
                    lambda x: x if not np.isinf(x) else np.NaN)

            # DATE COLUMNS FIX
            date_columns = [column for column in self.tables_dict[table_name].columns if column.endswith('Date')]

            if len(date_columns) != 0:
                print(f'\t{table_name} parsing date columns...')
                for column in date_columns:
                    self.tables_dict[table_name][column] = pd.to_datetime(self.tables_dict[table_name][column],
                                                                          infer_datetime_format=True,
                                                                          cache=True)

            # Set initial rows in stats df
            new_stats_row = {
                'Table': table_name,
                'Initial#rows': self.tables_dict[table_name].shape[0],
                'Final#rows': None,
                'Ratio': None
            }
            self.stats = self.stats.append(new_stats_row, ignore_index=True)

        print()

    def process_referencing_table(self, referencing):
        """
        This method is invoked recursively to drop rows with unresolvable foreign key constraints.
        It uses :func:`.MetaKagglePreprocessor.clean_referencing_table` to do the actual cleaning.
        Its purpose is to traverse the table relationships graph to take full relationship chains into account.
        It stops when it detects cycles.

        Args:
            referencing: the table with foreign keys to be analyzed.

        """

        print("### PREPROCESSING", referencing)

        self.already_visited.append(referencing)  # TODO: Try to avoid this
        referenced_list = self.constraints_df.loc[
            self.constraints_df['Table'] == referencing,
            'Referenced Table'].values

        # If referenced_list is NOT empty
        # I recursively process each referenced table and subsequently adjust the current
        if len(referenced_list) != 0:
            for referenced in referenced_list:
                print(self.already_visited)
                if referenced not in self.already_visited:
                    self.process_referencing_table(referenced)
                self.clean_referencing_table(referencing, referenced)

    def clean_referencing_table(self, referencing, referenced):
        """
        Given two tables, a referencing table and a referenced table, it cleans the former by removing all rows pointing
        to tuples from the second that cannot be found.

        Args:
            referencing: the table to be cleaned.
            referenced: the table whose rows are referenced by the table to be cleaned.

        """

        # From the constraints_df, I select info on the foreign keys of the referencing table
        # that point to the current referenced table.
        # In most cases I have only one of such foreign keys, but they might be more
        constraints_data = self.constraints_df.loc[
            ((self.constraints_df['Table'] == referencing) &
             (self.constraints_df['Referenced Table'] == referenced)),
            ['Foreign Key', 'Referenced Column']
        ]

        # I cycle through the foreign keys of the referencing table that point to the current referenced table
        for _, constraint in constraints_data.iterrows():

            fk = constraint['Foreign Key']
            print(f'\t\tForeign key: {fk}')

            rc = constraint['Referenced Column']
            print(f'\t\tReferenced column: {rc}')

            old_df_rows = self.tables_dict[referencing].shape[0]

            # For each foreign key, I update the referencing table
            # by removing rows that miss a corresponding row in the referenced table
            print(
                f'\tUpdating the referencing table "{referencing}" (foreign key "{fk}") '
                f'with the referenced table "{referenced}"')
            self.tables_dict[referencing] = self.tables_dict[referencing][
                (self.tables_dict[referencing][fk].isin(self.tables_dict[referenced][rc])) |
                (self.tables_dict[referencing][fk].isnull())
                ]

            # If any rows have been removed, I mark the constraints in which
            # this table is 'Referenced Table' as not solved
            if self.tables_dict[referencing].shape[0] != old_df_rows:
                self.constraints_df.loc[
                    (self.constraints_df['Referenced Table'] == referencing), 'IsSolved'] = False

            # Then I mark the constraint as solved
            self.constraints_df.loc[((self.constraints_df['Table'] == referencing) &
                                     (self.constraints_df['Referenced Table'] == referenced) &
                                     (self.constraints_df['Foreign Key'] == fk)), 'IsSolved'] = True

    def preprocess_mk(self):
        """
        This function handles the whole database population process.
        First, it runs the recursive preprocessing method of :class:`.MetaKagglePreprocessor` until foreign key constraints are
        solved for all tables. Then, it writes the preprocessed tables to the database. Finally, it prints summary stats
        about the filtering process to stdout.

        """

        print('### Executing basic preprocessing...')
        self.basic_preprocessing()

        print('### Executing referential integrity preprocessing...')

        # While all constraint fields 'IsSolved' is false
        while not self.constraints_df['IsSolved'].all():

            # Pre-process only referencing tables with constraints 'Not Solved' before writing
            # (and write those w/o fks)
            for value in (self.constraints_df[~ self.constraints_df['IsSolved']])['Table'].unique():
                print("\n")
                print("-------------")
                print("- New cycle -")
                print("-------------")

                self.process_referencing_table(value)
                self.already_visited = []

        # Final update of the stats table
        for _, row in self.stats.iterrows():
            self.stats.loc[self.stats['Table'] == row['Table'], 'Final#rows'] = self.tables_dict[row['Table']].shape[0]

        self.stats['Ratio'] = self.stats['Final#rows'] / self.stats['Initial#rows'] * 100
        self.stats['Ratio'] = self.stats['Ratio'].astype(float).round(decimals=2)

        return self.tables_dict, self.stats


if __name__ == '__main__':

    print("********************")
    print("*** LOADING DATA ***")
    print("********************")
    dl = DataLoader(config.constraints_file_path, config.meta_kaggle_path)

    print("**********************************")
    print("** TABLES PREPROCESSING STARTED **")
    print("**********************************")
    mk = MkPreprocessor(dl.get_tables_dict(), dl.get_constraints_df())
    processed_dict, stats = mk.preprocess_mk()

    # **************
    # SUMMARY PRINTS
    # **************

    print("CONSTRAINTS_DF")
    print(mk.constraints_df)
    print("\n")

    print("*************")
    print("*** STATS ***")
    print("*************\n")
    print(stats)

    print("******************")
    print("*** DATAFRAMES ***")
    print("******************")
    print(processed_dict)
