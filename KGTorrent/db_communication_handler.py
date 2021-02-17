"""
This module defines the class that handles the communication with the database via SQLAlchemy.
"""

import sys
from sqlalchemy.exc import IntegrityError

from KGTorrent.exceptions import DatabaseExistsError

import pandas as pd

# Imports to manage database
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, \
    create_database, \
    drop_database

# Imports to create table schemas
from sqlalchemy import (MetaData, Table, Column, Integer, String, Float,
                        DateTime, Boolean, Text, BigInteger)
from sqlalchemy.dialects.mysql import (MEDIUMTEXT, LONGTEXT)

# Imports for testing
import KGTorrent.config as config
from KGTorrent.mk_preprocessor import MkPreprocessor
from KGTorrent.data_loader import DataLoader


class DbCommunicationHandler:
    """
    This class creates an SQLAlchemy engine and has methods for creating, populating and querying
    a database with MetaKaggle data.
    """

    def __init__(self, db_username, db_password, db_host, db_port, db_name):
        """
        The constructor of this class creates the SQLAlchemy engine with provided arguments.

        Args:
            db_username: A valid username of the hosting MYSQL database
            db_password: The password related to the username
            db_host: An IP address of a MYSQL database
            db_port: The port of the MYSQL process on the host machine
            db_name: The name of the database to interact with
        """

        self._engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
            db_username,
            db_password,
            db_host,
            db_port,
            db_name
        ),
            pool_recycle=3600)

    def create_new_db(self, drop_if_exists=False):
        """
        This method creates a database with the provided name and builds schemas of MetaKaggle tables.
        It throws by default an exception when the database already exists in order to avoid an initialization
        called by mistake.

        Args:
            drop_if_exists: If True the database is dropped before creation. By default it is False.
        """

        def build_db_schema():
            """
            This function builds the schema of the KGTorrent MySQL database.
            """
            # Create the metadata object
            metadata = MetaData()

            # ====================
            # CREATE TABLE SCHEMAS
            # ====================

            competition_tags = Table('CompetitionTags', metadata,
                                     Column('Id', Integer(), primary_key=True),
                                     Column('CompetitionId', Integer(), nullable=False),
                                     Column('TagId', Integer(), nullable=False)
                                     )

            competitions = Table('Competitions', metadata,
                                 Column('Id', Integer(), primary_key=True),
                                 Column('Slug', String(255), unique=True, nullable=False),
                                 Column('Title', String(255), nullable=False),
                                 Column('SubTitle', Text()),
                                 Column('HostSegmentTitle', String(255), nullable=False),
                                 Column('ForumId', Integer()),
                                 Column('OrganizationId', Integer()),
                                 Column('CompetitionTypeId', Integer(), nullable=False),
                                 Column('HostName', String(255)),
                                 Column('EnabledDate', DateTime(), nullable=False),
                                 Column('DeadlineDate', DateTime(), nullable=False),
                                 Column('ProhibitNewEntrantsDeadlineDate', DateTime()),
                                 Column('TeamMergerDeadlineDate', DateTime()),
                                 Column('TeamModelDeadlineDate', DateTime()),
                                 Column('ModelSubmissionDeadlineDate', DateTime()),
                                 Column('FinalLeaderboardHasBeenVerified', Boolean(), nullable=False),
                                 Column('HasKernels', Boolean(), nullable=False),
                                 Column('OnlyAllowKernelSubmissions', Boolean(), nullable=False),
                                 Column('HasLeaderboard', Boolean(), nullable=False),
                                 Column('LeaderboardPercentage', Integer(), nullable=False),
                                 Column('LeaderboardDisplayFormat', Integer(), nullable=False),
                                 Column('EvaluationAlgorithmAbbreviation', String(255)),
                                 Column('EvaluationAlgorithmName', String(255)),
                                 Column('EvaluationAlgorithmDescription', MEDIUMTEXT),
                                 Column('EvaluationAlgorithmIsMax', Boolean()),
                                 Column('ValidationSetName', String(255)),
                                 Column('ValidationSetValue', String(255)),
                                 Column('MaxDailySubmissions', Integer(), nullable=False),
                                 Column('NumScoredSubmissions', Integer(), nullable=False),
                                 Column('MaxTeamSize', Integer()),
                                 Column('BanTeamMergers', Boolean(), nullable=False),
                                 Column('EnableTeamModels', Boolean(), nullable=False),
                                 Column('EnableSubmissionModelHashes', Boolean(), nullable=False),
                                 Column('EnableSubmissionModelAttachments', Boolean(), nullable=False),
                                 Column('RewardType', String(255)),
                                 Column('RewardQuantity', Integer()),
                                 Column('NumPrizes', Integer(), nullable=False),
                                 Column('UserRankMultiplier', Integer(), nullable=False),
                                 Column('CanQualifyTiers', Boolean(), nullable=False),
                                 Column('TotalTeams', Integer(), nullable=False),
                                 Column('TotalCompetitors', Integer(), nullable=False),
                                 Column('TotalSubmissions', Integer(), nullable=False)
                                 )

            dataset_tags = Table('DatasetTags', metadata,
                                 Column('Id', Integer(), primary_key=True),
                                 Column('DatasetId', Integer(), nullable=False),
                                 Column('TagId', Integer(), nullable=False)
                                 )

            dataset_versions = Table('DatasetVersions', metadata,
                                     Column('Id', Integer(), primary_key=True),
                                     Column('DatasetId', Integer(), nullable=False),
                                     Column('DatasourceVersionId', Integer()),
                                     Column('CreatorUserId', Integer(), nullable=False),
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

            dataset_votes = Table('DatasetVotes', metadata,
                                  Column('Id', Integer(), primary_key=True),
                                  Column('UserId', Integer(), nullable=False),
                                  Column('DatasetVersionId', Integer(), nullable=False),
                                  Column('VoteDate', DateTime(), nullable=False)
                                  )

            datasets = Table('Datasets', metadata,
                             Column('Id', Integer(), primary_key=True),
                             Column('CreatorUserId', Integer(), nullable=False),
                             Column('OwnerUserId', Integer()),
                             Column('OwnerOrganizationId', Integer()),
                             Column('CurrentDatasetVersionId', Integer()),
                             Column('CurrentDatasourceVersionId', Integer()),
                             Column('ForumId', Integer(), nullable=False),
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

            datasources = Table('Datasources', metadata,
                                Column('Id', Integer(), primary_key=True),
                                Column('CreatorUserId', Integer(), nullable=False),
                                Column('CreationDate', DateTime(), nullable=False),
                                Column('Type', Integer(), nullable=False),
                                Column('CurrentDatasourceVersionId', Integer(), nullable=False)
                                )

            forum_message_votes = Table('ForumMessageVotes', metadata,
                                        Column('Id', Integer(), primary_key=True),
                                        Column('ForumMessageId', Integer(), nullable=False),
                                        Column('FromUserId', Integer(), nullable=False),
                                        Column('ToUserId', Integer(), nullable=False),
                                        Column('VoteDate', DateTime(), nullable=False)
                                        )

            forum_messages = Table('ForumMessages', metadata,
                                   Column('Id', Integer(), primary_key=True),
                                   Column('ForumTopicId', Integer(), nullable=False),
                                   Column('PostUserId', Integer(), nullable=False),
                                   Column('PostDate', DateTime(), nullable=False),
                                   Column('ReplyToForumMessageId', Integer()),
                                   Column('Message', LONGTEXT),
                                   Column('Medal', Integer()),
                                   Column('MedalAwardDate', DateTime())
                                   )

            forum_topics = Table('ForumTopics', metadata,
                                 Column('Id', Integer(), primary_key=True),
                                 Column('ForumId', Integer(), nullable=False),
                                 Column('KernelId', Integer()),
                                 Column('LastForumMessageId', Integer()),
                                 Column('FirstForumMessageId', Integer()),
                                 Column('CreationDate', DateTime(), nullable=False),
                                 Column('LastCommentDate', DateTime(), nullable=False),
                                 Column('Title', String(255)),
                                 Column('IsSticky', Boolean(), nullable=False),
                                 Column('TotalViews', Integer(), nullable=False),
                                 Column('Score', Integer(), nullable=False),
                                 Column('TotalMessages', Integer(), nullable=False),
                                 Column('TotalReplies', Integer(), nullable=False)
                                 )

            forums = Table('Forums', metadata,
                           Column('Id', Integer(), primary_key=True),
                           Column('ParentForumId', Integer()),
                           Column('Title', String(255))
                           )

            kernel_languages = Table('KernelLanguages', metadata,
                                     Column('Id', Integer(), primary_key=True),
                                     Column('Name', String(255), unique=True, nullable=False),
                                     Column('DisplayName', String(255), nullable=False),
                                     Column('IsNotebook', Boolean(), nullable=False)
                                     )

            kernel_tags = Table('KernelTags', metadata,
                                Column('Id', Integer(), primary_key=True),
                                Column('KernelId', Integer(), nullable=False),
                                Column('TagId', Integer(), nullable=False)
                                )

            kernel_version_competition_sources = Table('KernelVersionCompetitionSources', metadata,
                                                       Column('Id', Integer(), primary_key=True),
                                                       Column('KernelVersionId', Integer(), nullable=False),
                                                       Column('SourceCompetitionId', Integer(), nullable=False)
                                                       )

            kernel_version_dataset_sources = Table('KernelVersionDatasetSources', metadata,
                                                   Column('Id', Integer(), primary_key=True),
                                                   Column('KernelVersionId', Integer(), nullable=False),
                                                   Column('SourceDatasetVersionId', Integer(), nullable=False)
                                                   )

            kernel_version_kernel_sources = Table('KernelVersionKernelSources', metadata,
                                                  Column('Id', Integer(), primary_key=True),
                                                  Column('KernelVersionId', Integer(), nullable=False),
                                                  Column('SourceKernelVersionId', Integer(), nullable=False)
                                                  )

            kernel_version_output_files = Table('KernelVersionOutputFiles', metadata,
                                                Column('Id', Integer(), primary_key=True),
                                                Column('KernelVersionId', Integer(), nullable=False),
                                                Column('FileName', String(255)),
                                                Column('ContentLength', BigInteger(), nullable=False),
                                                Column('ContentTypeExtension', String(255)),
                                                Column('CompressionTypeExtension', String(255))
                                                )

            kernel_versions = Table('KernelVersions', metadata,
                                    Column('Id', Integer(), primary_key=True),
                                    Column('ScriptId', Integer(), nullable=False),
                                    Column('ParentScriptVersionId', Integer()),
                                    Column('ScriptLanguageId', Integer(), nullable=False),
                                    Column('AuthorUserId', Integer(), nullable=False),
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

            kernel_votes = Table('KernelVotes', metadata,
                                 Column('Id', Integer(), primary_key=True),
                                 Column('UserId', Integer(), nullable=False),
                                 Column('KernelVersionId', Integer(), nullable=False),
                                 Column('VoteDate', DateTime(), nullable=False)
                                 )

            kernels = Table('Kernels', metadata,
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

            organizations = Table('Organizations', metadata,
                                  Column('Id', Integer(), primary_key=True),
                                  Column('Name', String(255), nullable=False),
                                  Column('Slug', String(255), unique=True, nullable=False),
                                  Column('CreationDate', DateTime(), nullable=False),
                                  Column('Description', Text())
                                  )

            submissions = Table('Submissions', metadata,
                                Column('Id', Integer(), primary_key=True),
                                Column('SubmittedUserId', Integer()),
                                Column('TeamId', Integer(), nullable=False),
                                Column('SourceKernelVersionId', Integer()),
                                Column('SubmissionDate', DateTime(), nullable=False),
                                Column('ScoreDate', DateTime()),
                                Column('IsAfterDeadline', Boolean(), nullable=False),
                                Column('PublicScoreLeaderboardDisplay', Float(52)),
                                Column('PublicScoreFullPrecision', Float(52)),
                                Column('PrivateScoreLeaderboardDisplay', Float(52)),
                                Column('PrivateScoreFullPrecision', Float(52))
                                )

            tags = Table('Tags', metadata,
                         Column('Id', Integer(), primary_key=True),
                         Column('ParentTagId', Integer()),
                         Column('Name', String(255), nullable=False),
                         Column('Slug', String(255), nullable=False),
                         Column('FullPath', String(255), nullable=False),
                         Column('Description', Text()),
                         Column('DatasetCount', Integer(), nullable=False),
                         Column('CompetitionCount', Integer(), nullable=False),
                         Column('KernelCount', Integer(), nullable=False)
                         )

            team_memberships = Table('TeamMemberships', metadata,
                                     Column('Id', Integer(), primary_key=True),
                                     Column('TeamId', Integer(), nullable=False),
                                     Column('UserId', Integer(), nullable=False),
                                     Column('RequestDate', DateTime())
                                     )

            teams = Table('Teams', metadata,
                          Column('Id', Integer(), primary_key=True),
                          Column('CompetitionId', Integer(), nullable=False),
                          Column('TeamLeaderId', Integer()),
                          Column('TeamName', String(255)),
                          Column('ScoreFirstSubmittedDate', DateTime()),
                          Column('LastSubmissionDate', DateTime()),
                          Column('PublicLeaderboardSubmissionId', Integer()),
                          Column('PrivateLeaderboardSubmissionId', Integer()),
                          Column('IsBenchmark', Boolean(), nullable=False),
                          Column('Medal', Integer()),
                          Column('MedalAwardDate', DateTime()),
                          Column('PublicLeaderboardRank', Integer()),
                          Column('PrivateLeaderboardRank', Integer())
                          )

            user_achievements = Table('UserAchievements', metadata,
                                      Column('Id', Integer(), primary_key=True),
                                      Column('UserId', Integer(), nullable=False),
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

            user_followers = Table('UserFollowers', metadata,
                                   Column('Id', Integer(), primary_key=True),
                                   Column('UserId', Integer(), nullable=False),
                                   Column('FollowingUserId', Integer(), nullable=False),
                                   Column('CreationDate', DateTime(), nullable=False)
                                   )

            user_organizations = Table('UserOrganizations', metadata,
                                       Column('Id', Integer(), primary_key=True),
                                       Column('UserId', Integer(), nullable=False),
                                       Column('OrganizationId', Integer(), nullable=False),
                                       Column('JoinDate', DateTime(), nullable=False)
                                       )

            users = Table('Users', metadata,
                          Column('Id', Integer(), primary_key=True),
                          Column('UserName', String(255), unique=True),
                          Column('DisplayName', String(255)),
                          Column('RegisterDate', DateTime(), nullable=False),
                          Column('PerformanceTier', Integer(), nullable=False)
                          )

            # Create all tables added to the metadata object
            metadata.create_all(self._engine)
            # END build_db_schema

        if database_exists(self._engine.url):
            if drop_if_exists:
                drop_database(self._engine.url)
            else:
                raise DatabaseExistsError(f'Database {self._engine.url.database} already exists.')
        create_database(self._engine.url, 'utf8mb4')
        build_db_schema()

    def db_exists(self):
        """
        This method checks whether the database exists.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        return database_exists(self._engine.url)

    def write_tables(self, tables_dict):
        """
        This method writes tables to the database by using the ``pandas.DataFrame.to_sql`` method.

        Args:
            tables_dict: The dictionary whose keys are the table names and whose values are the ``pandas.DataFrame`` tables.
        """

        for table_name in tables_dict.keys():

            # Format sql name
            sql_name = table_name.split('.')[0].lower()

            print('Writing "{}" to database...'.format(table_name))
            tables_dict[table_name].to_sql(sql_name,
                                           self._engine,
                                           if_exists='append',  # TODO: make a choice here
                                           index=False,
                                           chunksize=10000
                                           )

            print('"{}" written to database.\n'.format(table_name))

    def set_foreign_keys(self, constraints_df):
        """
        This method sets the foreign key constraints based on information provided by the related ``pandas.DataFrame``.

        Args:
            constraints_df: The ``pandas.DataFrame`` which contains the foreign key constraints information
        """

        for _, fk in constraints_df.iterrows():
            table_name = fk['Table'][:-4].lower()
            foreign_key = fk['Foreign Key']
            referenced_table = fk['Referenced Table'][:-4].lower()
            referenced_col = fk['Referenced Column']

            query = f'ALTER TABLE {table_name} ' \
                    f'ADD FOREIGN KEY ({foreign_key}) REFERENCES {referenced_table}({referenced_col});'

            print('Executing "{}"'.format(query))
            try:
                self._engine.execute(query)
            except IntegrityError as e:
                print("\t - INTEGRITY ERROR. Can't update table ", table_name, file=sys.stderr)

    def get_nb_identifiers(self, languages):
        """
        This method queries the database in order to retrieve slugs and identifiers of notebooks
        written in the provided languages.

        Args:
            languages: A string array of notebook languages present in Kaggle.

        Returns:
            nb_identifiers: The ``pandas.DataFrame`` containing notebook slugs and identifiers.
        """

        # Prepare the query
        query = 'SELECT ' \
                'users.UserName, ' \
                'kernels.CurrentUrlSlug, ' \
                'kernels.CurrentKernelVersionId ' \
                'FROM ' \
                '(((kernels INNER JOIN users ON kernels.AuthorUserId = users.Id) ' \
                'INNER JOIN kernelversions ON kernels.CurrentKernelVersionId = kernelversions.Id) ' \
                'INNER JOIN kernellanguages ON kernelversions.ScriptLanguageId = kernellanguages.Id) ' \
                f'WHERE '

        # Add where clause for each language
        for lang in languages:

            if lang is languages[0]:
                query = query + f'kernellanguages.name LIKE \'{lang}\' '

            else:
                query = query + f'OR kernellanguages.name LIKE \'{lang}\' '

        # Close the query
        query = query + ';'

        # Execute the query
        nb_identifiers = pd.read_sql(sql=query, con=self._engine)

        return nb_identifiers


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

    print("*************")
    print("*** STATS ***")
    print("*************\n")
    print(stats)

    print(f"## Connecting to {config.db_name} db on port {config.db_port} as user {config.db_username}")
    db_engine = DbCommunicationHandler(config.db_username,
                                       config.db_password,
                                       config.db_host,
                                       config.db_port,
                                       config.db_name)
    print("Connection established.")
    print("Initializing DB...")
    ans = 'yes'
    if db_engine.db_exists():
        ans = input("Are you sure to re-initialize db? [yes]")

    if ans == 'yes':

        db_engine.create_new_db(drop_if_exists=True)

        print("***************************")
        print("** DB POPULATION STARTED **")
        print("***************************")
        db_engine.write_tables(processed_dict)

        print("** APPLICATION OF CONSTRAINTS **")
        db_engine.set_foreign_keys(dl.get_constraints_df())

        print("** QUERING KERNELS TO DOWNLOAD **")
        kernels_ids = db_engine.get_nb_identifiers(config.nb_conf['languages'])
        print(kernels_ids.head())
