import os

from sqlalchemy import (MetaData, Table, Column, Integer, String, Float,
                        DateTime, Boolean, ForeignKey, create_engine, Text, BigInteger)
from sqlalchemy.dialects.mysql import MEDIUMTEXT

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

userAchievements = Table('UserAchievements', metadata,
                         Column('Id', Integer(), primary_key=True),
                         Column('UserId', Integer(), ForeignKey('Users.Id'), nullable=False),
                         Column('AchievementType', String(255), nullable=False),
                         Column('Tier', Integer(), nullable=False),
                         Column('TierAchievementDate', DateTime()),
                         Column('Points', Integer(), nullable=False),
                         Column('CurrentRanking', Integer()),
                         Column('HighestRanking', Integer()),
                         Column('TotalGold', Integer(), nullable=False),
                         Column('TotalSilver', Integer(), nullable=False),
                         Column('TotalBronze', Integer(), nullable=False)
                         )

kernels = Table('Kernels', metadata,
                Column('Id', Integer(), primary_key=True),
                Column('AuthorUserId', Integer(), ForeignKey('Users.Id'), nullable=False),
                Column('CurrentKernelVersionId', Integer(), ForeignKey('KernelVersions.Id')),
                Column('ForkParentKernelVersionId', Integer()),  # ForeignKey('KernelVersions.Id') removed
                # TODO: Set foreign key for the field "ForumTopicId"
                Column('ForumTopicId', Integer()),
                Column('FirstKernelVersionId', Integer()),  # ForeignKey('KernelVersions.Id') removed
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
                       Column('ParentScriptVersionId', Integer()),  # ForeignKey("KernelVersions.Id") removed
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
                    Column('Id', Integer(), primary_key=True),
                    Column('UserId', Integer(), nullable=False),
                    Column('KernelVersionId', Integer(), nullable=False),
                    Column('VoteDate', DateTime(), nullable=False)
                    )

tags = Table('Tags', metadata,
             Column('Id', Integer(), primary_key=True),
             Column('ParentTagId', Integer()),  # Foreign key omitted
             Column('Name', String(255), nullable=False),
             Column('Slug', String(255), nullable=False),
             Column('FullPath', String(255), nullable=False),
             Column('Description', Text()),
             Column('DatasetCount', Integer(), nullable=False),
             Column('CompetitionCount', Integer(), nullable=False),
             Column('KernelCount', Integer(), nullable=False)
             )

kernelTags = Table('KernelTags', metadata,
                   Column('Id', Integer(), primary_key=True),
                   Column('KernelId', Integer(), ForeignKey('Kernels.Id'), nullable=False),
                   Column('TagId', Integer(), ForeignKey('Tags.Id'), nullable=False)
                   )

datasets = Table('Datasets', metadata,
                 Column('Id', Integer(), primary_key=True),
                 Column('CreatorUserId', Integer(), ForeignKey('Users.Id'), nullable=False),
                 Column('OwnerUserId', Integer()),  # Foreign key omitted
                 Column('OwnerOrganizationId', Integer()),  # Foreign key omitted
                 # TODO: Add foreign key to CurrentDatasetVersion
                 Column('CurrentDatasetVersionId', Integer()),
                 Column('CurrentDatasourceVersionId', Integer()),  # Foreign key omitted
                 Column('ForumId', Integer(), nullable=False),  # Foreign key omitted
                 Column('Type', Integer(), nullable=False),
                 Column('CreationDate', DateTime(), nullable=False),
                 Column('ReviewDate', DateTime()),
                 Column('FeatureDate', DateTime()),
                 Column('LastActivityDate', DateTime(), nullable=False),
                 Column('TotalViews', Integer(), nullable=False),
                 Column('TotalDownloads', Integer(), nullable=False),
                 Column('TotalVotes', Integer(), nullable=False),
                 Column('TotalKernels', Integer(), nullable=False)
                 )

datasetTags = Table('DatasetTags', metadata,
                    Column('Id', Integer(), primary_key=True),
                    Column('DatasetId', Integer(), ForeignKey('Datasets.Id'), nullable=False),
                    Column('TagId', Integer(), ForeignKey('Tags.Id'), nullable=False)
                    )

datasetVersions = Table('DatasetVersions', metadata,
                        Column('Id', Integer(), primary_key=True),
                        Column('DatasetId', Integer(), ForeignKey('Datasets.Id'), nullable=False),
                        Column('DatasourceVersionId', Integer()),  # Foreign key omitted
                        Column('CreatorUserId', Integer(), ForeignKey('Users.Id'), nullable=False),
                        Column('LicenseName', String(255), nullable=False),
                        Column('CreationDate', DateTime(), nullable=False),
                        Column('VersionNumber', Integer()),
                        Column('Title', String(255)),
                        Column('Slug', String(255), nullable=False),
                        Column('Subtitle', String(255)),
                        Column('Description', MEDIUMTEXT),
                        Column('VersionNotes', Text()),
                        Column('TotalCompressedBytes', BigInteger()),
                        Column('TotalUncompressedBytes', BigInteger())
                        )

datasetVotes = Table('DatasetVotes', metadata,
                     Column('Id', Integer(), primary_key=True),
                     Column('UserId', Integer(), ForeignKey('Users.Id'), nullable=False),
                     Column('DatasetVersionId', Integer(), ForeignKey('DatasetVersions.Id'), nullable=False),
                     Column('VoteDate', DateTime(), nullable=False)
                     )

kernelVersionDatasetSources = Table('KernelVersionDatasetSources', metadata,
                                    Column('Id', Integer(), primary_key=True),
                                    Column('KernelVersionId', Integer(), ForeignKey('KernelVersions.Id'),
                                           nullable=False),
                                    Column('SourceDatasetVersionId', ForeignKey('DatasetVersions.Id'), nullable=False)
                                    )

metadata.create_all(engine)
