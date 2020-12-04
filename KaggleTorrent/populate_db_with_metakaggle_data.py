"""
This module does the mapping of data from the MetaKaggle dataset to the KaggleTorrent relational database.
Use it once you have created the database schema by running the `build_db_schema` module.
"""

import os

import pandas as pd
import numpy as np
from sqlalchemy.exc import IntegrityError

import KaggleTorrent.config as config
from KaggleTorrent.db_connection_handler import DbConnectionHandler


# PRIVATE UTILITY FUNCTIONS

def check_table_emptiness(table_name, engine):
    """
    Checks if the database table of name `table_name` is empty.

    Args:
        table_name (str): the name of the table to be checked,
        which corresponds to the name of the .csv file from which its data is derived (omitting the extension).
        engine (Engine): the SQLAlchemy engine used to connect to the KaggleTorrent database

    Returns:
        bool: True if the database table is empty, False otherwise.

    """
    with engine.connect() as connection:
        rp = connection.execute('SELECT * FROM {} LIMIT 1'.format(table_name))
        result = rp.first()

        if result is not None:
            return False
        else:
            return True


def set_foreign_keys(sqlalchemy_engine, constraints_file_path):
    con = sqlalchemy_engine.connect()
    # TODO: Read this file outside of this function (it is needed also by the MetaKagglePreprocessor constructor)
    constraints_df = pd.read_excel(constraints_file_path, sheet_name='Foglio2')

    for _, fk in constraints_df.iterrows():
        table_name = fk['Table'][:-4].lower()
        foreign_key = fk['Foreign Key']
        referenced_table = fk['Referenced Table'][:-4].lower()
        referenced_col = fk['Referenced Column']

        query = f'ALTER TABLE {table_name} ' \
                f'ADD FOREIGN KEY ({foreign_key}) REFERENCES {referenced_table}({referenced_col});'

        print('Executing "{}"'.format(query))
        try:
            con.execute(query)
        except IntegrityError as e:
            print('\033[91m' + "\t - INTEGRITY ERROR. Can't update table ", table_name, '\033[0m')


def parse_dates(df):
    # TODO: document this method

    date_columns = [column for column in df.columns if column.endswith('Date')]

    if len(date_columns) != 0:
        print('\t\t\tParsing date columns...')
        for column in date_columns:
            df[column] = pd.to_datetime(df[column], infer_datetime_format=True, cache=True)

    return df


# MAIN CLASS

class MetaKagglePreprocessor:

    def __init__(self, constraints_file_path, meta_kaggle_path, sqlalchemy_engine):

        self.engine = sqlalchemy_engine
        self.meta_kaggle_path = meta_kaggle_path

        # Dataframe containing constraints info:
        # (Referencing Table, Foreign Key, Referenced Table, Referenced Column)
        # TODO: change to read_csv; maybe this file should be read outside of this constructor
        self.constraints_df = pd.read_excel(constraints_file_path, sheet_name='Foglio2')
        self.constraints_df['IsSolved'] = False

        # Keep track tables that have been already visited during the current recursive call
        self.already_visited = []

        # Dictionary of dataframes that need to be processed
        self.dataframes = {}

        # Dataframe containing info on row loss after referential integrity checks on referencing tables
        self.stats = pd.DataFrame(columns=['Table', 'Initial#rows', 'Final#rows', 'Written'])

    def load_table(self, table_name, is_ready_for_db=False):
        """

        Args:
            table_name:
            is_ready_for_db:

        Returns:

        """

        if is_ready_for_db:
            df = pd.read_csv(os.path.join(self.meta_kaggle_path, table_name))

            if 'ForumMessageVotes' in table_name:
                print('\t\t Fix indexing...')
                df.drop_duplicates(subset=['Id'], inplace=True)

            df = parse_dates(df)
            self.write_table(table_name, df)

        print('\t- Searching the table...')
        if table_name not in self.dataframes:

            print('\t- Table not loaded yet, reading the csv...')
            self.dataframes[table_name] = pd.read_csv(os.path.join(self.meta_kaggle_path, table_name))

            # SPECIFIC TABLES FIX
            if 'ForumMessageVotes' in table_name:
                print('\t\t Fix indexing...')
                self.dataframes[table_name].drop_duplicates(subset=['Id'], inplace=True)

            if 'Submissions' in table_name:
                print('\t\t Fix precision columns')

                self.dataframes[table_name]['PublicScoreLeaderboardDisplay'] = self.dataframes[table_name][
                    'PublicScoreLeaderboardDisplay'].map(lambda x: round(float(x), 3)).map(
                    lambda x: x if not np.isinf(x) else np.NaN)

                self.dataframes[table_name]['PublicScoreFullPrecision'] = self.dataframes[table_name][
                    'PublicScoreFullPrecision'].map(lambda x: round(float(x), 3)).map(
                    lambda x: x if not np.isinf(x) else np.NaN)

                self.dataframes[table_name]['PrivateScoreLeaderboardDisplay'] = self.dataframes[table_name][
                    'PrivateScoreLeaderboardDisplay'].map(lambda x: round(float(x), 3)).map(
                    lambda x: x if not np.isinf(x) else np.NaN)

                self.dataframes[table_name]['PrivateScoreFullPrecision'] = self.dataframes[table_name][
                    'PrivateScoreFullPrecision'].map(lambda x: round(float(x), 3)).map(
                    lambda x: x if not np.isinf(x) else np.NaN)

            # DATE COLUMNS FIX
            self.dataframes[table_name] = parse_dates(self.dataframes[table_name])
            new_stats_row = {
                'Table': table_name,
                'Initial#rows': self.dataframes[table_name].shape[0],
                'Final#rows': None,
                'Written': False
            }
            self.stats = self.stats.append(new_stats_row, ignore_index=True)
            print('\t- Table loaded.')

        else:
            print('\t- Table already loaded.')

    def write_table(self, table_name, df=None):
        """

        Args:
            table_name:
            df:

        Returns:

        """

        if df is None:
            df = self.dataframes[table_name]

        print('Writing "{}" to database...'.format(table_name))
        df.to_sql(table_name[:-4].lower(),
                  self.engine,
                  if_exists='append',  # TODO: make a choice here
                  index=False,
                  chunksize=10000)

        self.stats.loc[self.stats['Table'] == table_name, 'Written'] = True
        print('"{}" written to database.\n'.format(table_name))

        del df

    def process_referencing_table(self, referencing):
        """

        Args:
            referencing:

        Returns:

        """

        print("### READING", referencing)

        self.load_table(referencing)
        self.already_visited.append(referencing)  # TODO: Try to avoid this
        referenced_list = self.constraints_df.loc[
            self.constraints_df['Table'] == referencing,
            'Referenced Table'].values

        # If referenced_list is empty and this table has never been written to the db, then I write it now
        if len(referenced_list) == 0 and \
                (not self.stats.loc[self.stats['Table'] == referencing, 'Written'].values[0]):
            print("### WRITING", referencing)
            self.write_table(referencing)

        # Otherwise, I recursively process each referenced table and subsequently adjust the current
        else:
            for referenced in referenced_list:
                print(self.already_visited)
                if referenced not in self.already_visited:
                    self.process_referencing_table(referenced)
                self.clean_referencing_table(referencing, referenced)

    def clean_referencing_table(self, referencing, referenced):
        """

        Args:
            referencing:
            referenced:

        Returns:

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

            # For each foreign key, I update the referencing table
            # by removing rows that miss a corresponding row in the referenced table
            print(
                f'\tUpdating the referencing table "{referencing}" (foreign key "{fk}") '
                f'with the referenced table "{referenced}"')
            self.dataframes[referencing] = self.dataframes[referencing][
                (self.dataframes[referencing][fk].isin(self.dataframes[referenced][rc])) |
                (self.dataframes[referencing][fk].isnull())
            ]

            # Then I mark the constraint as solved
            self.constraints_df.loc[((self.constraints_df['Table'] == referencing) &
                                     (self.constraints_df['Referenced Table'] == referenced) &
                                     (self.constraints_df['Foreign Key'] == fk)), 'IsSolved'] = True


def populate_db(mk):
    """

    Args:
        mk:

    Returns:

    """

    print("*****************************")
    print("** DB POPULATION STARTED 1 **")
    print("*****************************")

    # Process tables before writing
    # (and write those w/o fks)
    for value in mk.constraints_df['Table'].unique():
        print("\n")
        print("-------------")
        print("- New cycle -")
        print("-------------")

        mk.process_referencing_table(value)
        mk.already_visited = []

    print("*****************************")
    print("** DB POPULATION STARTED 2 **")
    print("*****************************")

    # Process tables before writing
    # (and write those w/o fks)
    for value in mk.constraints_df['Table'].unique():
        print("\n")
        print("-------------")
        print("- New cycle -")
        print("-------------")

        mk.process_referencing_table(value)
        mk.already_visited = []

    print("*****************************")
    print("** DB POPULATION STARTED 3 **")
    print("*****************************")

    # Process tables before writing
    # (and write those w/o fks)
    for value in mk.constraints_df['Table'].unique():
        print("\n")
        print("-------------")
        print("- New cycle -")
        print("-------------")

        mk.process_referencing_table(value)
        mk.already_visited = []

    # Write processed tables to the database
    for value in mk.constraints_df['Table'].unique():
        if not mk.stats.loc[mk.stats['Table'] == value, 'Written'].values[0]:
            mk.write_table(value)

    # **************
    # SUMMARY PRINTS
    # **************

    print("CONSTRAINTS_DF")
    print(mk.constraints_df)
    print("\n")

    # Final update of the stats table
    for _, row in mk.stats.iterrows():
        mk.stats.loc[mk.stats['Table'] == row['Table'], 'Final#rows'] = mk.dataframes[row['Table']].shape[0]

    mk.stats['Ratio'] = mk.stats['Final#rows'] / mk.stats['Initial#rows'] * 100
    mk.stats['Ratio'] = mk.stats['Ratio'].astype(float).round(decimals=2)

    print("*************")
    print("*** STATS ***")
    print("*************\n")
    print(mk.stats)


if __name__ == "__main__":
    # Create DB engine
    db_connection_handler = DbConnectionHandler()
    e = db_connection_handler.create_sqlalchemy_engine()

    # Populate the database
    mk_preprocessor = MetaKagglePreprocessor(config.constraints_file_path,
                                             meta_kaggle_path=config.meta_kaggle_path,
                                             sqlalchemy_engine=e)
    populate_db(mk_preprocessor)
    set_foreign_keys(sqlalchemy_engine=e, constraints_file_path=config.constraints_file_path)
