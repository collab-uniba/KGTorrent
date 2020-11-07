"""
This module does the mapping of data from the MetaKaggle dataset to the KaggleTorrent relational database.
Use it once you have created the database schema by running the `build_db_schema` module.
"""

import os

import pandas as pd

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


# MAIN CLASS

class MetaKagglePreprocessor:

    def __init__(self, constraints_file_path, meta_kaggle_path, sqlalchemy_engine):

        self.engine = sqlalchemy_engine
        self.meta_kaggle_path = meta_kaggle_path

        # Dataframe containing constraints info:
        # (Referencing Table, Foreign Key, Referenced Table, Referenced Column)
        self.constraints_df = pd.read_excel(constraints_file_path)  # TODO: change to read_csv
        self.constraints_df['IsSolved'] = False
        print(self.constraints_df)

        # Series of lists indexed by referenced table names:
        # Each list stores all the tables referencing their index
        self.referencing_tables_lists = self.constraints_df.groupby(by='Referenced Table')['Table'].apply(list).iloc[
                                        ::-1]

        # Dictionary to store dates that have already been parsed
        self.dates_dict = {}

        # Dictionary of dataframes that need to be processed
        self.dataframes = {}

        # Dataframe containing info on row loss after referential integrity checks on referencing tables
        self.stats = pd.DataFrame(columns=['Table', 'Initial#rows', 'Final#rows'])

    def parse_dates(self, df):
        # TODO: document this method

        date_columns = [column for column in df.columns if column.endswith('Date')]

        if len(date_columns) != 0:
            print('\t\t\tParsing date columns...')
            for column in date_columns:
                self.dates_dict.update({date: pd.to_datetime(date, format=None) for date in df[column].unique()})
                df[column] = df[column].map(self.dates_dict)

        return df

    def write_table_to_db(self, table_name, df=None):
        # TODO: document this method

        # TODO: find the right place to check table emptiness (for now
        # I assume that the database is always empty at the beginning of the process).
        # Check whether the database table has already been populated
        # if check_table_emptiness(table_name=table_name[:-4],
        #                            engine=self.engine):

        if df is None:
            df = self.dataframes[table_name]

        print('Writing "{}" to database...'.format(table_name))
        df.to_sql(table_name[:-4].lower(),
                  self.engine,
                  if_exists='append',  # TODO: make a choice here
                  index=False,
                  chunksize=10000)

        print('"{}" written to database.\n'.format(table_name))

        print('number of elements of self.dataframes before del:', len(self.dataframes))  # TODO: delete this print asap
        del df
        print('number of elements of self.dataframes after del:', len(self.dataframes))  # TODO: delete this print asap

    def load_table(self, table_name, is_ready_for_db=False):
        # TODO: document this method

        if is_ready_for_db:
            df = pd.read_csv(os.path.join(self.meta_kaggle_path, table_name))
            df = self.parse_dates(df)
            self.write_table_to_db(table_name, df)

        print('\t- Searching the table...')
        if table_name not in self.dataframes:

            print('\t- Table not loaded yet, reading the csv...')
            self.dataframes[table_name] = pd.read_csv(os.path.join(self.meta_kaggle_path, table_name))
            self.dataframes[table_name] = self.parse_dates(self.dataframes[table_name])
            new_stats_row = {
                'Table': table_name,
                'Initial#rows': self.dataframes[table_name].shape[0],
                'Final#rows': None
            }
            self.stats = self.stats.append(new_stats_row, ignore_index=True)
            print('\t- Table loaded.')

        else:
            print('\t- Table already loaded.')


def populate_db(mk, meta_kaggle_path):
    # First of all, I load and write to the db all the tables that do not have foreign keys
    # TODO: probably this step can be avoided (and also the need for `meta_kaggle_path` in input to this function)
    for root, dirs, files in os.walk(meta_kaggle_path):
        for table_name in filter(lambda x: (x.endswith('.csv') and
                                            x not in set(mk.constraints_df['Table']) and
                                            x not in set(mk.constraints_df['Referenced Table'])), files):
            print(table_name)
            print("Loading table READY FOR DB!")
            mk.load_table(table_name, is_ready_for_db=True)

    # The index of `referencing_tables_lists` consists of all the referenced tables
    # ordered by the number of referencing tables
    # I cycle through all the referenced tables, starting from the most referenced
    for referenced_table in mk.referencing_tables_lists.index:

        # First of all, I make sure that the current referenced table has already been loaded
        # and I load it otherwise
        print(f'\nCurrent referenced table: "{referenced_table}"')
        mk.load_table(referenced_table)

        # Then I cycle through all the tables referencing the current referenced table
        print('\t\tSorted referencing tables:', sorted(mk.referencing_tables_lists[referenced_table],
                                                       key=lambda
                                                           x: 0 if x == referenced_table else 1))  # TODO: remove this print asap

        for referencing_table in sorted(mk.referencing_tables_lists[referenced_table],
                                        key=lambda x: 0 if x == referenced_table else 1):

            # I make sure that the current referencing table has already been loaded
            # and I load it otherwise
            print(f'\tCurrent referencing table: "{referencing_table}"')
            mk.load_table(referencing_table)

            # From the constraints_df, I select info on the foreign keys of the referencing table
            # that point to the current referenced table.
            # In most cases I have only one of such foreign keys, but they might be more
            constraints_data = mk.constraints_df.loc[
                ((mk.constraints_df['Table'] == referencing_table) &
                 (mk.constraints_df['Referenced Table'] == referenced_table)),
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
                print('\tUpdating the referencing table')
                mk.dataframes[referencing_table] = mk.dataframes[referencing_table][mk.dataframes[referencing_table][fk]
                    .isin(mk.dataframes[referenced_table][rc])]

                # Then I mark the constraint as solved
                mk.constraints_df.loc[((mk.constraints_df['Table'] == referencing_table) &
                                       (mk.constraints_df['Referenced Table'] == referenced_table) &
                                       (mk.constraints_df['Foreign Key'] == fk)), 'IsSolved'] = True

        # Now I can write the referenced table to the db
        mk.write_table_to_db(referenced_table)

        for referencing_table in mk.referencing_tables_lists[referenced_table]:
            # If all foreing keys for the current referencing table are solved at this point,
            # then write the table to the db
            if all(mk.constraints_df.loc[(mk.constraints_df['Table'] == referencing_table), 'IsSolved']):
                print(f'\tAll fk solved for "{referencing_table}"')
                print(mk.constraints_df.loc[(mk.constraints_df[
                                                 'Table'] == referencing_table), 'IsSolved'])  # TODO: remove this print asap
                mk.write_table_to_db(referencing_table)

    # Final update of the stats table
    for _, row in mk.stats.iterrows():
        mk.stats.loc[mk.stats['Table'] == row['Table'], 'Final#rows'] = mk.dataframes[row['Table']].shape[0]

    mk.stats['Ratio'] = mk.stats['Final#rows'] / mk.stats['Initial#rows'] * 100
    mk.stats['Ratio'] = mk.stats['Ratio'].astype(float).round(decimals=2)

    print("*************")
    print("*** STATS ***")
    print("*************\n")
    print(mk.stats)


# def __read_data(meta_kaggle_path, file_name):
#     """
#     Reads data from the MetaKaggle .csv file given as input into a Pandas dataframe.
#
#     Since the parsing of dates from the MetaKaggle dataset is computationally expensive,
#     a serialized version of each Pandas dataframe is saved upon parsing completion.
#
#     The goal of this function is to return a Pandas dataframe containing data from the specified dataset file.
#     It checks whether such file has already been preprocessed.
#     When this is the case (i.e., when the corresponding bz2 compressed pickle file is available in the dataset folder)
#     it just deserializes and returns it.
#     Conversely, when the file is being read for the first time, this function returns an unprocessed version
#     of it as a Pandas dataframe.
#     Its preprocessing (i.e., the parsing of date fields, etc.) will be handled by the function __csv_to_sql.
#
#     Args:
#         meta_kaggle_path (str): the path to the folder containing the MetaKaggle dataset on your machine
#         file_name (str): the name of the specific .csv file from the MetaKaggle dataset that you want to read
#
#     Returns:
#         df (DataFrame): the Pandas dataframe containing data from the MetaKaggle file in input
#         preprocessed (bool): a flag indicating whether the returned dataframe needs preprocessing or not.
#
#     """
#
#     full_path = os.path.join(meta_kaggle_path, file_name)
#     pickle_path = os.path.join(meta_kaggle_path, '{}.bz2'.format(file_name[:-4]))
#
#     print('Reading "{}"...'.format(file_name))
#     if os.path.isfile(pickle_path):
#         preprocessed = True
#         df = pd.read_pickle(pickle_path)
#     else:
#         df = pd.read_csv(full_path)
#         preprocessed = False
#     return df, preprocessed


# def __csv_to_sql(meta_kaggle_path, file_name, sqlalchemy_engine, referenced_tables=None):
#     """
#
#     Args:
#         meta_kaggle_path (str): the path to the folder containing the MetaKaggle dataset on your machine
#         file_name (str): the name of the specific .csv file from the MetaKaggle dataset that you want to read
#         sqlalchemy_engine (Engine): the SQLAlchemy engine connected to the KaggleTorrent database
#         date_columns (list): the list of dataframe columns containing dates to be parsed
#         referenced_tables (dict): the list of dataframe columns that will be assigned foreign keys in the relational database
#
#     """
#
#     # Check whether the database table has already been populated
#     if __check_table_emptiness(table_name=file_name[:-4],
#                                engine=sqlalchemy_engine):
#
#         full_path = os.path.join(meta_kaggle_path, file_name)
#         print('Reading "{}"...'.format(file_name))
#         df = pd.read_csv(full_path)
#
#         date_columns = [column for column in df.columns if column.endswith('Date')]
#
#         # If the table is being read for the first time and needs preprocessing...
#         if len(date_columns) != 0 or referenced_tables is not None:
#
#             print('Pre-processing "{}"...'.format(file_name))
#
#             # Date columns parsing
#             if len(date_columns) != 0:
#                 print('\t- Parsing date columns...')
#                 for column in date_columns:
#                     # TODO: pd.to_datetime has still to convert the same dates many times
#                     dates = {date: pd.to_datetime(date, format=None) for date in df[column].unique()}
#                     df[column] = df[column].map(dates)
#
#             # Restricting rows so that referential integrity holds
#             if referenced_tables is not None:
#                 print('\t- Checking the table for referential integrity...')
#
#                 for referenced_table in referenced_tables:
#
#                     full_ref_table_path = os.path.join(meta_kaggle_path, referenced_table)
#                     rt = pd.read_csv(full_ref_table_path)
#
#                     # Take notes on the original table cardinality to estimate the data loss involved in the merge.
#                     print('\t\tOriginal shape: {}.'.format(df.shape))
#
#                     # For each foreign key, keep only rows that respect the constraint
#                     # TODO: Check the correctness of this operation (maybe write a test here)
#                     for fk in referenced_tables[referenced_table]:
#                         print('\t\tJoining "{}"...'.format(referenced_table))
#                         # join = pd.merge(df, rt, left_on=fk, right_on='Id')
#                         # df = df[df[fk].isin(join['Id_y'])]
#                         df = df[df[fk].isin(rt['Id'])]
#                         # Take notes on the new table cardinality to estimate the data loss involved in the merge.
#                         print('\t\tNew shape: {}.'.format(df.shape))
#
#         # Write data to the corresponding database table
#         print('Writing "{}"...'.format(file_name))
#         df.to_sql(file_name[:-4].lower(),
#                   sqlalchemy_engine,
#                   if_exists='append',
#                   index=False,
#                   chunksize=10000)
#         print('"{}" written to database.\n'.format(file_name))
#
#     else:
#         # TODO : trovare il modo di aggiornare le tuple quando la table è già piena
#         #  (altrimenti da errore di integrità con 'append' perchè la primary key viene reinserita
#         #  e da errore con 'replace' per via dei vincoli di integrità di altre tabelle che fanno
#         #  riferimento a quella attuale)
#         #  NB : in mysql 'INSERT ... ON DUPLICATE KEY [INGORE, UPDATE]' per evitare questo problema.
#         #  Su SQLAlchemy suggeriscono di usare il merge per ovviare al problema
#         #  (https://stackoverflow.com/questions/6611563/sqlalchemy-on-duplicate-key-update)
#         #  Un'altra soluzione è quella di fare 'DROP DATABASE IF EXISTS kaggle_torrent' e
#         #  successivamente ricrearlo vuoto
#         print('Table "{}" already filled.'.format(file_name[:-4]))


# # POPULATION FUNCTION
#
# def populate_db(sqlalchemy_engine, meta_kaggle_path):
#     """
#     Populates the KaggleTorrent database by importing data from the MetaKaggle dataset.
#
#     Args:
#         sqlalchemy_engine (Engine): the SQLAlchemy engine connected to the KaggleTorrent database
#         meta_kaggle_path (str): the path to the folder containing the MetaKaggle dataset on your machine
#     """
#
#     print("DB POPULATION STARTED...\n")
#
#     # USERS
#     __csv_to_sql(meta_kaggle_path, 'Users.csv', sqlalchemy_engine)
#
#     # USER ACHIEVEMENTS
#     ref_tables = {
#         'Users.csv': [
#             'UserId'
#         ]
#     }
#     __csv_to_sql(meta_kaggle_path, 'UserAchievements.csv', sqlalchemy_engine,
#                  referenced_tables=ref_tables)
#
#     # KERNEL LANGUAGES
#     __csv_to_sql(meta_kaggle_path, 'KernelLanguages.csv', sqlalchemy_engine)
#
#     # KERNEL VERSIONS
#     ref_tables = {
#         'Users.csv': [
#             'AuthorUserId'
#         ],
#         # 'Kernels.csv': [
#         #     'ScriptId'
#         # ],
#         'KernelLanguages.csv': [
#             'ScriptLanguageId'
#         ]
#     }
#     __csv_to_sql(meta_kaggle_path, 'KernelVersions.csv', sqlalchemy_engine,
#                  referenced_tables=ref_tables)
#
#     # KERNELS
#     ref_tables = {
#         'Users.csv': [
#             'AuthorUserId'
#         ],
#         'KernelVersions.csv': [
#             'CurrentKernelVersionId'
#         ]
#     }
#     __csv_to_sql(meta_kaggle_path, 'Kernels.csv', sqlalchemy_engine,
#                  referenced_tables=ref_tables)

# # KERNEL VOTES
# ref_tables = {
#     'Users.csv': [
#         'UserId'
#     ],
#     'KernelVersions.csv': [
#         'KernelVersionId'
#     ]
# }
# __csv_to_sql(meta_kaggle_path, 'KernelVotes.csv', sqlalchemy_engine,
#              referenced_tables=ref_tables)
#
# # TAGS
# __csv_to_sql(meta_kaggle_path, 'Tags.csv', sqlalchemy_engine)
#
# # KERNEL TAGS
# ref_tables = {
#     'Tags.csv': [
#         'TagId'
#     ],
#     'Kernels.csv': [
#         'KernelId'
#     ]
# }
# __csv_to_sql(meta_kaggle_path, 'KernelTags.csv', sqlalchemy_engine,
#              referenced_tables=ref_tables)
#
# # DATASETS
# ref_tables = {
#     'Users.csv': [
#         'CreatorUserId'
#     ],
#     # 'DatasetVersions.csv': [
#     #     'CurrentDatasetVersionId'
#     # ]
# }
# __csv_to_sql(meta_kaggle_path, 'Datasets.csv', sqlalchemy_engine,
#              referenced_tables=ref_tables)
#
# # DATASET VERSIONS.CSV
# ref_tables = {
#     'Users.csv': [
#         'CreatorUserId'
#     ],
#     'Datasets.csv': [
#         'DatasetId'
#     ]
# }
# __csv_to_sql(meta_kaggle_path, 'DatasetVersions.csv', sqlalchemy_engine,
#              referenced_tables=ref_tables)
#
# # DATASET TAGS
# ref_tables = {
#     'Tags.csv': [
#         'TagId'
#     ],
#     'Datasets.csv': [
#         'DatasetId'
#     ]
# }
# __csv_to_sql(meta_kaggle_path, 'DatasetTags.csv', sqlalchemy_engine,
#              referenced_tables=ref_tables)
#
# # DATASET VOTES
# ref_tables = {
#     'Users.csv': [
#         'UserId'
#     ],
#     'DatasetVersions.csv': [
#         'DatasetVersionId'
#     ]
# }
# __csv_to_sql(meta_kaggle_path, 'DatasetVotes.csv', sqlalchemy_engine,
#              referenced_tables=ref_tables)
#
# # KERNEL VERSION - DATASET SOURCES
# ref_tables = {
#     'KernelVersions.csv': [
#         'KernelVersionId'
#     ],
#     'DatasetVersions.csv': [
#         'SourceDatasetVersionId'
#     ]
# }
# __csv_to_sql(meta_kaggle_path, 'KernelVersionDatasetSources.csv', sqlalchemy_engine,
#              referenced_tables=ref_tables)

# print("DB POPULATION COMPLETED")


if __name__ == "__main__":
    # Create DB engine
    db_connection_handler = DbConnectionHandler()
    e = db_connection_handler.create_sqlalchemy_engine()

    # Populate the database
    mk_preprocessor = MetaKagglePreprocessor(config.constraints_file_path,
                                             meta_kaggle_path=config.meta_kaggle_path,
                                             sqlalchemy_engine=e)
    populate_db(mk_preprocessor, meta_kaggle_path=config.meta_kaggle_path)
