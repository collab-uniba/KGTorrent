import os

import pandas as pd
from sqlalchemy import (create_engine)


# UTILITY FUNCTIONS
def read_data(dir_path, file_name):
    full_path = os.path.join(dir_path, file_name)
    pickle_path = os.path.join(dir_path, '{}.bz2'.format(file_name[:-4]))

    print('Reading "{}"...'.format(file_name))
    if os.path.isfile(pickle_path):
        preprocessed = True
        df = pd.read_pickle(pickle_path)
    else:
        df = pd.read_csv(full_path)
        preprocessed = False
    return df, preprocessed


def check_table_emptiness(table_name):
    with engine.connect() as connection:
        rp = connection.execute('SELECT * FROM {} LIMIT 1'.format(table_name))
        result = rp.first()

        if result is not None:
            return False
        else:
            return True


def csv_to_sql(dir_path, file_name, date_columns=[], referenced_tables=None):
    """Procedure to map the csv files of the meta-kaggle dataset to db tables"""

    if check_table_emptiness(file_name[:-4]):

        df, preprocessed = read_data(dir_path, file_name)

        if not preprocessed and ((len(date_columns) > 0) or referenced_tables is not None):
            print('Pre-processing "{}"...'.format(file_name))
            if len(date_columns) > 0:
                print('\t- Convert the date format...')
                df[date_columns] = df[date_columns].apply(pd.to_datetime)
            if referenced_tables is not None:
                print('\t- Clean the table for referential integrity...')
                for referenced_table in referenced_tables:
                    rt, prep = read_data(dir_path, referenced_table)
                    # if not prep:
                    #     raise TableNotPreprocessedError('Table "{}" must be preprocessed.'
                    #                                     'Import it before "{}"'.format(referenced_table,
                    #                                                                    file_name))
                    print('\t\tOriginal shape: {}.'.format(df.shape))
                    for fk in referenced_tables[referenced_table]:
                        print('\t\tJoining "{}"...'.format(referenced_table))
                        join = pd.merge(df, rt, left_on=fk, right_on='Id')
                        df = df[df[fk].isin(join['Id_y'])]
                        print('\t\tNew shape: {}.'.format(df.shape))

            print('Serializing "{}"...'.format(file_name))
            pickle_path = os.path.join(dir_path, '{}.bz2'.format(file_name[:-4]))
            df.to_pickle(pickle_path)

        print('Writing "{}"...'.format(file_name))
        df.to_sql(file_name[:-4], engine, if_exists='append', index=False)
        print('"{}" written to database.\n'.format(file_name))

    else:
        print('Table "{}" already filled.'.format(file_name[:-4]))


def populate_db():
    print("DB POPULATION STARTED...\n")

    dir_path = '/Users/luigiquaranta/Downloads/meta-kaggle'

    # Users.csv
    date_cols = ['RegisterDate']
    csv_to_sql(dir_path, 'Users.csv', date_cols)

    # UserAchievements.csv
    date_cols = [
        'TierAchievementDate'
    ]
    ref_tables = {
        'Users.csv': [
            'UserId'
        ]
    }
    csv_to_sql(dir_path, 'UserAchievements.csv', date_cols, ref_tables)

    # KernelLanguages.csv
    csv_to_sql(dir_path, 'KernelLanguages.csv')

    # KernelVersions.csv
    date_cols = [
        'CreationDate',
        'EvaluationDate'
    ]
    ref_tables = {
        'Users.csv': [
            'AuthorUserId'
        ],
        # 'Kernels.csv': [
        #     'ScriptId'
        # ],
        'KernelLanguages.csv': [
            'ScriptLanguageId'
        ]
    }
    csv_to_sql(dir_path, 'KernelVersions.csv', date_cols, ref_tables)

    # Kernels.csv
    date_cols = [
        'CreationDate',
        'EvaluationDate',
        'MadePublicDate',
        'MedalAwardDate'
    ]
    ref_tables = {
        'Users.csv': [
            'AuthorUserId'
        ],
        'KernelVersions.csv': [
            'CurrentKernelVersionId'
        ]
    }
    csv_to_sql(dir_path, 'Kernels.csv', date_cols, ref_tables)

    # KernelVotes.csv
    date_cols = [
        'VoteDate'
    ]
    ref_tables = {
        'Users.csv': [
            'UserId'
        ],
        'KernelVersions.csv': [
            'KernelVersionId'
        ]
    }
    csv_to_sql(dir_path, 'KernelVotes.csv', date_cols, ref_tables)

    # Tags
    csv_to_sql(dir_path, 'Tags.csv')

    # KernelTags
    ref_tables = {
        'Tags.csv': [
            'TagId'
        ],
        'Kernels.csv': [
            'KernelId'
        ]
    }
    csv_to_sql(dir_path, 'KernelTags.csv', referenced_tables=ref_tables)

    # Datasets.csv
    date_cols = [
        'CreationDate',
        'ReviewDate',
        'FeatureDate',
        'LastActivityDate'
    ]
    ref_tables = {
        'Users.csv': [
            'CreatorUserId'
        ],
        # 'DatasetVersions.csv': [
        #     'CurrentDatasetVersionId'
        # ]
    }
    csv_to_sql(dir_path, 'Datasets.csv', date_cols, ref_tables)

    # DatasetVersions.csv
    date_cols = [
        'CreationDate'
    ]
    ref_tables = {
        'Users.csv': [
            'CreatorUserId'
        ],
        'Datasets.csv': [
            'DatasetId'
        ]
    }
    csv_to_sql(dir_path, 'DatasetVersions.csv', date_cols, ref_tables)

    # DatasetTags.csv
    ref_tables = {
        'Tags.csv': [
            'TagId'
        ],
        'Datasets.csv': [
            'DatasetId'
        ]
    }
    csv_to_sql(dir_path, 'DatasetTags.csv', referenced_tables=ref_tables)

    # DatasetVotes.csv
    date_cols = [
        'VoteDate'
    ]
    ref_tables = {
        'Users.csv': [
            'UserId'
        ],
        'DatasetVersions.csv': [
            'DatasetVersionId'
        ]
    }
    csv_to_sql(dir_path, 'DatasetVotes.csv', date_cols, ref_tables)

    # KernelVersionDatasetSources.csv
    ref_tables = {
        'KernelVersions.csv': [
            'KernelVersionId'
        ],
        'DatasetVersions.csv': [
            'SourceDatasetVersionId'
        ]
    }
    csv_to_sql(dir_path, 'KernelVersionDatasetSources.csv', referenced_tables=ref_tables)


if __name__ == "__main__":
    mysql_username = os.environ['MYSQL_USER']
    mysql_password = os.environ['MYSQL_PWD']

    engine = create_engine('mysql+pymysql://{}:{}@localhost:3306/kaggle_torrent'
                           '?charset=utf8mb4'.format(mysql_username, mysql_password), pool_recycle=3600)
    populate_db()
