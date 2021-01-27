"""This module builds the schema for the companion database of the KGTorrent dataset."""

from sqlalchemy import (MetaData, Table, Column, Integer, String, Float,
                        DateTime, Boolean, Text, BigInteger)
from sqlalchemy.dialects.mysql import (MEDIUMTEXT, LONGTEXT)

from KGTorrent.db_connection_handler import DbConnectionHandler


class DbSchema:
    """
    The constructor of this class builds the schema of the KGTorrent MySQL database.

    Args:
                sqlalchemy_engine (Engine): the SQLAlchemy engine used to connect to the database.

    """

    def __init__(self, sqlalchemy_engine):
        # Create the metadata object
        metadata = MetaData()

        # ====================
        # CREATE TABLE SCHEMAS
        # ====================

        self.competition_tags = Table('CompetitionTags', metadata,
                                      Column('Id', Integer(), primary_key=True),
                                      Column('CompetitionId', Integer(), nullable=False),
                                      Column('TagId', Integer(), nullable=False)
                                      )

        self.competitions = Table('Competitions', metadata,
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

        self.dataset_tags = Table('DatasetTags', metadata,
                                  Column('Id', Integer(), primary_key=True),
                                  Column('DatasetId', Integer(), nullable=False),
                                  Column('TagId', Integer(), nullable=False)
                                  )

        self.dataset_versions = Table('DatasetVersions', metadata,
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

        self.dataset_votes = Table('DatasetVotes', metadata,
                                   Column('Id', Integer(), primary_key=True),
                                   Column('UserId', Integer(), nullable=False),
                                   Column('DatasetVersionId', Integer(), nullable=False),
                                   Column('VoteDate', DateTime(), nullable=False)
                                   )

        self.datasets = Table('Datasets', metadata,
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

        self.datasources = Table('Datasources', metadata,
                                 Column('Id', Integer(), primary_key=True),
                                 Column('CreatorUserId', Integer(), nullable=False),
                                 Column('CreationDate', DateTime(), nullable=False),
                                 Column('Type', Integer(), nullable=False),
                                 Column('CurrentDatasourceVersionId', Integer(), nullable=False)
                                 )

        self.forum_message_votes = Table('ForumMessageVotes', metadata,
                                         Column('Id', Integer(), primary_key=True),
                                         Column('ForumMessageId', Integer(), nullable=False),
                                         Column('FromUserId', Integer(), nullable=False),
                                         Column('ToUserId', Integer(), nullable=False),
                                         Column('VoteDate', DateTime(), nullable=False)
                                         )

        self.forum_messages = Table('ForumMessages', metadata,
                                    Column('Id', Integer(), primary_key=True),
                                    Column('ForumTopicId', Integer(), nullable=False),
                                    Column('PostUserId', Integer(), nullable=False),
                                    Column('PostDate', DateTime(), nullable=False),
                                    Column('ReplyToForumMessageId', Integer()),
                                    Column('Message', LONGTEXT),
                                    Column('Medal', Integer()),
                                    Column('MedalAwardDate', DateTime())
                                    )

        self.forum_topics = Table('ForumTopics', metadata,
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

        self.forums = Table('Forums', metadata,
                            Column('Id', Integer(), primary_key=True),
                            Column('ParentForumId', Integer()),
                            Column('Title', String(255))
                            )

        self.kernel_languages = Table('KernelLanguages', metadata,
                                      Column('Id', Integer(), primary_key=True),
                                      Column('Name', String(255), unique=True, nullable=False),
                                      Column('DisplayName', String(255), nullable=False),
                                      Column('IsNotebook', Boolean(), nullable=False)
                                      )

        self.kernel_tags = Table('KernelTags', metadata,
                                 Column('Id', Integer(), primary_key=True),
                                 Column('KernelId', Integer(), nullable=False),
                                 Column('TagId', Integer(), nullable=False)
                                 )

        self.kernel_version_competition_sources = Table('KernelVersionCompetitionSources', metadata,
                                                        Column('Id', Integer(), primary_key=True),
                                                        Column('KernelVersionId', Integer(), nullable=False),
                                                        Column('SourceCompetitionId', Integer(), nullable=False)
                                                        )

        self.kernel_version_dataset_sources = Table('KernelVersionDatasetSources', metadata,
                                                    Column('Id', Integer(), primary_key=True),
                                                    Column('KernelVersionId', Integer(), nullable=False),
                                                    Column('SourceDatasetVersionId', Integer(), nullable=False)
                                                    )

        self.kernel_version_kernel_sources = Table('KernelVersionKernelSources', metadata,
                                                   Column('Id', Integer(), primary_key=True),
                                                   Column('KernelVersionId', Integer(), nullable=False),
                                                   Column('SourceKernelVersionId', Integer(), nullable=False)
                                                   )

        self.kernel_version_output_files = Table('KernelVersionOutputFiles', metadata,
                                                 Column('Id', Integer(), primary_key=True),
                                                 Column('KernelVersionId', Integer(), nullable=False),
                                                 Column('FileName', String(255)),
                                                 Column('ContentLength', BigInteger(), nullable=False),
                                                 Column('ContentTypeExtension', String(255)),
                                                 Column('CompressionTypeExtension', String(255))
                                                 )

        self.kernel_versions = Table('KernelVersions', metadata,
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

        self.kernel_votes = Table('KernelVotes', metadata,
                                  Column('Id', Integer(), primary_key=True),
                                  Column('UserId', Integer(), nullable=False),
                                  Column('KernelVersionId', Integer(), nullable=False),
                                  Column('VoteDate', DateTime(), nullable=False)
                                  )

        self.kernels = Table('Kernels', metadata,
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

        self.organizations = Table('Organizations', metadata,
                                   Column('Id', Integer(), primary_key=True),
                                   Column('Name', String(255), nullable=False),
                                   Column('Slug', String(255), unique=True, nullable=False),
                                   Column('CreationDate', DateTime(), nullable=False),
                                   Column('Description', Text())
                                   )

        self.submissions = Table('Submissions', metadata,
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

        self.tags = Table('Tags', metadata,
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

        self.team_memberships = Table('TeamMemberships', metadata,
                                      Column('Id', Integer(), primary_key=True),
                                      Column('TeamId', Integer(), nullable=False),
                                      Column('UserId', Integer(), nullable=False),
                                      Column('RequestDate', DateTime())
                                      )

        self.teams = Table('Teams', metadata,
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

        self.user_achievements = Table('UserAchievements', metadata,
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

        self.user_followers = Table('UserFollowers', metadata,
                                    Column('Id', Integer(), primary_key=True),
                                    Column('UserId', Integer(), nullable=False),
                                    Column('FollowingUserId', Integer(), nullable=False),
                                    Column('CreationDate', DateTime(), nullable=False)
                                    )

        self.user_organizations = Table('UserOrganizations', metadata,
                                        Column('Id', Integer(), primary_key=True),
                                        Column('UserId', Integer(), nullable=False),
                                        Column('OrganizationId', Integer(), nullable=False),
                                        Column('JoinDate', DateTime(), nullable=False)
                                        )

        self.users = Table('Users', metadata,
                           Column('Id', Integer(), primary_key=True),
                           Column('UserName', String(255), unique=True),
                           Column('DisplayName', String(255)),
                           Column('RegisterDate', DateTime(), nullable=False),
                           Column('PerformanceTier', Integer(), nullable=False)
                           )

        # Create all tables added to the metadata object
        metadata.create_all(sqlalchemy_engine)


if __name__ == "__main__":
    # Create DB engine
    db_connection_handler = DbConnectionHandler()
    e = db_connection_handler.create_sqlalchemy_engine()

    # Build the database schema
    DbSchema(sqlalchemy_engine=e)
