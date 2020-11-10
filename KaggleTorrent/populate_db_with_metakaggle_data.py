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


def set_foreign_keys(sqlalchemy_engine, constraints_file_path):
    con = sqlalchemy_engine.connect()
    constraints_df = pd.read_excel(
        constraints_file_path)  # TODO: Read the file outside (it is needed also by the MetaKagglePreprocessor constructor)

    for _, fk in constraints_df.iterrows():
        table_name = fk['Table'][:-4].lower()
        foreign_key = fk['Foreign Key']
        referenced_table = fk['Referenced Table']
        referenced_col = fk['Referenced Column']

        con.execute(f'ALTER TABLE {table_name}'
                    f'ADD FOREIGN KEY ({foreign_key}) REFERENCES {referenced_table}({referenced_col});')


# MAIN CLASS

class MetaKagglePreprocessor:

    def __init__(self, constraints_file_path, meta_kaggle_path, sqlalchemy_engine):

        self.engine = sqlalchemy_engine
        self.meta_kaggle_path = meta_kaggle_path

        # Dataframe containing constraints info:
        # (Referencing Table, Foreign Key, Referenced Table, Referenced Column)
        self.constraints_df = pd.read_excel(constraints_file_path)  # TODO: change to read_csv and read the file outside
        self.constraints_df['IsSolved'] = False
        self.constraints_df['IsWritten'] = False
        print(self.constraints_df)

        # Series of lists indexed by referenced table names:
        # Each list stores all the tables referencing their index
        self.referencing_tables_lists = self.constraints_df.groupby(by='Referenced Table')['Table'].apply(list).iloc[
                                        ::-1]

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
                df[column] = pd.to_datetime(df[column], infer_datetime_format=True, cache=True)

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

        self.constraints_df.loc[self.constraints_df['Table'] == table_name, 'IsWritten'] = True
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
    # for root, dirs, files in os.walk(meta_kaggle_path):
    #     for table_name in filter(lambda x: (x.endswith('.csv') and
    #                                         x not in set(mk.constraints_df['Table']) and
    #                                         x not in set(mk.constraints_df['Referenced Table'])), files):
    #         print(table_name)
    #         print("Loading table READY FOR DB!")
    #         mk.load_table(table_name, is_ready_for_db=True)

    # The index of `referencing_tables_lists` consists of all the referenced tables
    # ordered by the number of referencing tables
    # I cycle through all the referenced tables, starting from the most referenced
    for referenced_table in mk.referencing_tables_lists.index:

        # First of all, I make sure that the current referenced table has already been loaded
        # and I load it otherwise
        print(f'\nCurrent referenced table: "{referenced_table}"')
        mk.load_table(referenced_table)

        # Then I cycle through all the tables referencing the current referenced table
        # starting from the referenced table if it references itself
        # (and thus is in the list of the referencing tables too)
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

        # Now I can write the referenced table to the db # TODO: remove this old comment
        # Now, if the referenced table does not play the role of a referencing table in another relationship,
        # I can write it to the db

        if referenced_table not in mk.constraints_df.loc[mk.constraints_df['IsSolved'] == False, 'Table'].values:
            print('Nothing else to solve for the current referenced table.')
            print(mk.constraints_df[['Table', 'IsSolved', 'IsWritten']])  # TODO: remove this print
            mk.write_table_to_db(referenced_table)

        for referencing_table in mk.referencing_tables_lists[referenced_table]:
            # If all foreign keys for the current referencing table are solved at this point,
            # then write the table to the db
            if all(mk.constraints_df.loc[(mk.constraints_df['Table'] == referencing_table), 'IsSolved']):
                print(f'\tAll fk solved for "{referencing_table}"')
                print(mk.constraints_df[['Table', 'IsSolved', 'IsWritten']])  # TODO: remove this print
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


if __name__ == "__main__":
    # Create DB engine
    db_connection_handler = DbConnectionHandler()
    e = db_connection_handler.create_sqlalchemy_engine()

    # Populate the database
    mk_preprocessor = MetaKagglePreprocessor(config.constraints_file_path,
                                             meta_kaggle_path=config.meta_kaggle_path,
                                             sqlalchemy_engine=e)

    populate_db(mk_preprocessor, meta_kaggle_path=config.meta_kaggle_path)
