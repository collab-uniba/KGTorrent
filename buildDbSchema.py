import os
import pandas as pd
from sqlalchemy import (MetaData, Table, Column, Integer, String, Float,
                        DateTime, Boolean, ForeignKey, create_engine)

# Before executing this script, execute in the mysql client the following command
# CREATE DATABASE IF NOT EXISTS kaggle_torrent CHARACTER SET utf8mb4

mysql_username = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PWD']
engine = create_engine('mysql+pymysql://{}:{}@localhost:3306/kaggle_torrent'
                       '?charset=utf8mb4'.format(mysql_username, mysql_password), pool_recycle=3600)

metadata = MetaData()

kernels = Table('kernels', metadata,
                Column('Id', Integer(), primary_key=True),
                Column('AuthorUserId', Integer(), nullable=False),
                Column('CurrentKernelVersionId', Integer()),
                Column('ForkParentKernelVersionId', Integer()),
                Column('ForumTopicId', Integer()),
                Column('FirstKernelVersionId', Integer()),
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

metadata.create_all(engine)


kernels_df = pd.read_csv('~/Downloads/meta-kaggle/Kernels.csv')
kernels_df[['CreationDate', 'EvaluationDate', 'MadePublicDate', 'MedalAwardDate']] = \
    kernels_df[['CreationDate', 'EvaluationDate', 'MadePublicDate', 'MedalAwardDate']].apply(pd.to_datetime)
print(kernels_df.dtypes)
kernels_df.to_sql('kernels', engine, if_exists='append', index=False)
