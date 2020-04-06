import os

import pandas as pd
from sqlalchemy import (MetaData, Table, Column, Integer, String, Float,
                        DateTime, Boolean, ForeignKey, create_engine)

mysql_username = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PWD']

# Before executing this script, execute in the mysql client the following command
# CREATE DATABASE IF NOT EXISTS kaggle_torrent CHARACTER SET utf8mb4;


engine = create_engine('mysql+pymysql://{}:{}@localhost:3306/kaggle_torrent'
                       '?charset=utf8mb4'.format(mysql_username, mysql_password), pool_recycle=3600)

metadata = MetaData()

users = Table('Users', metadata,
              Column('Id', Integer(), primary_key=True),
              Column('UserName', String(255), unique=True),
              Column('DisplayName', String(255)),
              Column('RegisterDate', DateTime(), nullable=False),
              Column('PerformanceTier', Integer(), nullable=False)
              )

# TODO: Set foreign key for the field "ForumTopicId"
kernels = Table('Kernels', metadata,
                Column('Id', Integer(), primary_key=True),
                Column('AuthorUserId', Integer(), ForeignKey('Users.Id'), nullable=False),
                Column('CurrentKernelVersionId', Integer(), ForeignKey('KernelVersions.Id')),
                Column('ForkParentKernelVersionId', Integer(), ForeignKey('KernelVersions.Id')),
                Column('ForumTopicId', Integer()),
                Column('FirstKernelVersionId', Integer(), ForeignKey('KernelVersions.Id')),
                Column('CreationDate', DateTime()),
                Column('EvaluationDate', DateTime()),
                Column('MadePublicDate', DateTime()),
                Column('IsProjectLanguageTemplate', Boolean(), nullable=False),
                Column('CurrentUrlSlug', String(255)),
                Column('Medal', Float()),
                Column('MedalAwardDate', DateTime()),
                Column('TotalViews', Integer(), nullable=False),
                Column('TotalComments', Integer(), nullable=False),
                Column('TotalVotes', Integer(), nullable=False)
                )

kernelLanguages = Table('KernelLanguages', metadata,
                        Column('Id', Integer(), primary_key=True),
                        Column('Name', String(255), unique=True, nullable=False),
                        Column('DisplayName', String(255), nullable=False),
                        Column('IsNotebook', Boolean(), nullable=False)
                        )

kernelVersions = Table('KernelVersions', metadata,
                       Column('Id', Integer(), primary_key=True),
                       # TODO: reinsert FK Constraint ForeignKey("Kernels.Id") for "ScriptId"
                       Column('ScriptId', Integer(), nullable=False),
                       Column('ParentScriptVersionId', Integer(), ForeignKey("KernelVersions.Id")),
                       Column('ScriptLanguageId', Integer(), ForeignKey('KernelLanguages.Id'), nullable=False),
                       Column('AuthorUserId', Integer(), ForeignKey('Users.Id'), nullable=False),
                       Column('CreationDate', DateTime(), nullable=False),
                       Column('VersionNumber', Integer()),
                       Column('Title', String(255)),
                       Column('EvaluationDate', DateTime()),
                       Column('IsChange', Boolean(), nullable=False),
                       Column('TotalLines', Integer()),
                       Column('LinesInsertedFromPrevious', Integer()),
                       Column('LinesChangedFromPrevious', Integer()),
                       Column('LinesUnchangedFromPrevious', Integer()),
                       Column('LinesInsertedFromFork', Integer()),
                       Column('LinesDeletedFromFork', Integer()),
                       Column('LinesChangedFromFork', Integer()),
                       Column('LinesUnchangedFromFork', Integer()),
                       Column('TotalVotes', Integer(), nullable=False)
                       )

kernelVotes = Table('KernelVotes', metadata,
                    Column('Id', Integer(), nullable=False),
                    Column('UserId', Integer(), nullable=False),
                    Column('KernelVersionId', Integer(), nullable=False),
                    Column('VoteDate', DateTime(), nullable=False)
)


metadata.create_all(engine)

# Procedure to map csv files to db tables
def csv_to_sql(csv_path, date_columns):
    file_name = os.path.basename(csv_path)
    print('Reading "{}"...'.format(file_name))
    df = pd.read_csv(csv_path)

    if len(date_columns) > 0:
        print('Pre-processing "{}"...'.format(file_name))
        df[date_columns] = df[date_columns].apply(pd.to_datetime)

    print('Writing "{}"...'.format(file_name))
    df.to_sql(file_name[:-4], engine, if_exists='append', index=False)
    print('"{}" written to database.\n'.format(file_name))



# Procedure to map csv files to db tables
def csv_to_pickle_to_sql(dir_path, file_name, date_columns):

    full_path = os.path.join(dir_path, file_name)
    pickle_path = os.path.join(dir_path, '{}.bz2'.format(file_name[:-4]))

    print('Reading "{}"...'.format(file_name))
    if os.path.isfile(pickle_path):
        df = pd.read_pickle(pickle_path)
    else:
        df = pd.read_csv(full_path)

        if len(date_columns) > 0:
            print('Pre-processing "{}"...'.format(file_name))
            df[date_columns] = df[date_columns].apply(pd.to_datetime)

        print('Serializing "{}"...'.format(file_name))
        df.to_pickle(pickle_path)


    print('Writing "{}"...'.format(file_name))
    df.to_sql(file_name[:-4], engine, if_exists='append', index=False)
    print('"{}" written to database.\n'.format(file_name))



DIR_PATH = '/Users/luigiquaranta/Downloads/meta-kaggle'

# Users.csv
date_columns = ['RegisterDate']
csv_to_pickle_to_sql(DIR_PATH, 'Users.csv', date_columns)


# KernelLanguages.csv
date_columns = []
csv_to_pickle_to_sql(DIR_PATH, 'KernelLanguages.csv', date_columns)


# KernelVersions.csv
date_columns = [
    'CreationDate',
    'EvaluationDate'
]
csv_to_pickle_to_sql(DIR_PATH, 'KernelVersions.csv', date_columns)

# # Kernels.csv
# csv_path = '~/Downloads/meta-kaggle/Kernels.csv'
# date_columns = [
#     'CreationDate',
#     'EvaluationDate',
#     'MadePublicDate',
#     'MedalAwardDate'
# ]
# csv_to_sql(csv_path, date_columns)


