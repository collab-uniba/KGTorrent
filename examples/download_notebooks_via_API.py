"""
This script queries the KaggleTorrent database and downloads the selected subset notebooks via the Kaggle API.

**Warning**: Before executing this script, you must run the following command in your MySQL client:

    ``CREATE DATABASE IF NOT EXISTS kaggle_torrent CHARACTER SET utf8mb4;``
"""

from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.rest import ApiException
from sqlalchemy import select, desc, and_

# from KaggleTorrent.build_db_schema import (users, kernels, kernelLanguages, kernelVersions)
from KaggleTorrent.build_db_schema import DbSchema
# Configuration
# TODO: Make this constant a command line argument.
from KaggleTorrent.db_connection_handler import DbConnectionHandler

DEST_FOLDER = './API_data/'

# Kaggle API Authentication
api = KaggleApi()
api.authenticate()

# Create DB engine
db_connection_handler = DbConnectionHandler()
engine = db_connection_handler.create_sqlalchemy_engine()

# Build the database schema
db_schema = DbSchema(sqlalchemy_engine=engine)

# Query the database
with engine.connect() as connection:
    # Prepare the query
    columns = [db_schema.users.c.UserName + '/' + db_schema.kernels.c.CurrentUrlSlug,
               db_schema.kernels.c.TotalVotes,
               db_schema.kernels.c.TotalViews]

    s = select(columns)
    s = s.select_from(
        db_schema.users.join(db_schema.kernels)
            .join(db_schema.kernel_versions)
            .join(db_schema.kernel_languages))
    s = s.where(
        and_(
            db_schema.kernels.c.Medal == 3,
            # users.c.PerformanceTier == 5,
            db_schema.kernel_languages.c.Name == 'IPython Notebook'
        )
    )
    s = s.order_by(desc(db_schema.kernels.c.TotalVotes), desc(db_schema.kernels.c.TotalViews))

    # TODO: this is just a POC. Remove this LIMIT statement once the query is verified.
    s = s.limit(20)

    # Execute the query
    rp = connection.execute(s)

# Download selected notebooks through the Kaggle API
for r in rp:
    print('Downloading "{}"...'.format(r[0]))
    print('kernels.c.TotalVotes: {}'.format(r[1]))
    print('kernels.c.TotalViews: {}'.format(r[2]))
    try:
        api.kernels_pull(r[0], DEST_FOLDER)
    except ApiException:
        print('\tERROR: the kernel "{}" could not be downloaded.'.format(r[0]))
        continue
