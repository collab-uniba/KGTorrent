"""
This module builds the db schema.

N.B.: Before executing this script, you must create the KaggleTorrent database
by running the following command in your MySQL client::

    CREATE DATABASE IF NOT EXISTS kaggle_torrent CHARACTER SET utf8mb4;

"""

from sqlalchemy import (MetaData, Table, Column, Integer, String, Float,
                        DateTime, Boolean, ForeignKey, Text, BigInteger)
from sqlalchemy.dialects.mysql import MEDIUMTEXT

from KaggleTorrent.db_connection_handler import DbConnectionHandler


class DbSchema:

    def __init__(self, sqlalchemy_engine):
        """
            By constructing an object of type DbSchema,
            this constructor builds the schema of the KaggleTorrent database.

            N.B.: To be able to call this constructor, the KaggleTorrent database
            must already exist in your DBMS.
            If it does not exist yet, run the following command in your MySQL client::

                CREATE DATABASE IF NOT EXISTS kaggle_torrent CHARACTER SET utf8mb4;

            Args:
                sqlalchemy_engine (Engine): the SQLAlchemy engine used to connect to the KaggleTorrent database

            """

        # TODO: this class should implement the singleton pattern.

        # Create the metadata object
        metadata = MetaData()

        # ====================
        # CREATE TABLE SCHEMAS
        # ====================
        self.users = Table('Users', metadata,
                           Column('Id', Integer(), primary_key=True),
                           Column('UserName', String(255), unique=True),
                           Column('DisplayName', String(255)),
                           Column('RegisterDate', DateTime(), nullable=False),
                           Column('PerformanceTier', Integer(), nullable=False)
                           )

        self.user_achievements = Table('UserAchievements', metadata,
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

        self.kernels = Table('Kernels', metadata,
                             Column('Id', Integer(), primary_key=True),
                             Column('AuthorUserId', Integer(), ForeignKey('Users.Id'), nullable=False),
                             Column('CurrentKernelVersionId', Integer()),
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

        self.kernel_languages = Table('KernelLanguages', metadata,
                                      Column('Id', Integer(), primary_key=True),
                                      Column('Name', String(255), unique=True, nullable=False),
                                      Column('DisplayName', String(255), nullable=False),
                                      Column('IsNotebook', Boolean(), nullable=False)
                                      )

        self.kernel_versions = Table('KernelVersions', metadata,
                                     Column('Id', Integer(), primary_key=True),
                                     # TODO: reinsert FK Constraint ForeignKey("Kernels.Id") for "ScriptId"
                                     Column('ScriptId', Integer(), nullable=False),
                                     Column('ParentScriptVersionId', Integer()),
                                     # ForeignKey("KernelVersions.Id") removed
                                     Column('ScriptLanguageId', Integer(), ForeignKey('KernelLanguages.Id'),
                                            nullable=False),
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

        self.kernel_votes = Table('KernelVotes', metadata,
                                  Column('Id', Integer(), primary_key=True),
                                  Column('UserId', Integer(), nullable=False),
                                  Column('KernelVersionId', Integer(), nullable=False),
                                  Column('VoteDate', DateTime(), nullable=False)
                                  )

        self.tags = Table('Tags', metadata,
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

        self.kernel_tags = Table('KernelTags', metadata,
                                 Column('Id', Integer(), primary_key=True),
                                 Column('KernelId', Integer(), ForeignKey('Kernels.Id'), nullable=False),
                                 Column('TagId', Integer(), ForeignKey('Tags.Id'), nullable=False)
                                 )

        self.datasets = Table('Datasets', metadata,
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

        self.dataset_tags = Table('DatasetTags', metadata,
                                  Column('Id', Integer(), primary_key=True),
                                  Column('DatasetId', Integer(), ForeignKey('Datasets.Id'), nullable=False),
                                  Column('TagId', Integer(), ForeignKey('Tags.Id'), nullable=False)
                                  )

        self.dataset_versions = Table('DatasetVersions', metadata,
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

        self.dataset_votes = Table('DatasetVotes', metadata,
                                   Column('Id', Integer(), primary_key=True),
                                   Column('UserId', Integer(), ForeignKey('Users.Id'), nullable=False),
                                   Column('DatasetVersionId', Integer(), ForeignKey('DatasetVersions.Id'),
                                          nullable=False),
                                   Column('VoteDate', DateTime(), nullable=False)
                                   )

        self.kernel_version__dataset_sources = Table('KernelVersionDatasetSources', metadata,
                                                     Column('Id', Integer(), primary_key=True),
                                                     Column('KernelVersionId', Integer(),
                                                            ForeignKey('KernelVersions.Id'),
                                                            nullable=False),
                                                     Column('SourceDatasetVersionId', ForeignKey('DatasetVersions.Id'),
                                                            nullable=False)
                                                     )

        # Create all tables added to the metadata object
        metadata.create_all(sqlalchemy_engine)


if __name__ == "__main__":
    # Create DB engine
    db_connection_handler = DbConnectionHandler()
    e = db_connection_handler.create_sqlalchemy_engine()

    # Build the database schema
    DbSchema(sqlalchemy_engine=e)
