"""
This script queries the KaggleTorrent database and downloads the selected subset notebooks via the Kaggle API.

**Warning**: Before executing this script, you must run the following command in your MySQL client:

    ``CREATE DATABASE IF NOT EXISTS kaggle_torrent CHARACTER SET utf8mb4;``
"""

import os

from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.rest import ApiException
from sqlalchemy import create_engine, select, desc, and_

from KaggleTorrent.buildDbSchema import (users, kernels, kernelLanguages, kernelVersions)

# Configuration
# TODO: Make this constant a command line argument.
DEST_FOLDER = './API_data/'

# Kaggle API Authentication
api = KaggleApi()
api.authenticate()

# Database connection
mysql_username = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PWD']
engine = create_engine('mysql+pymysql://{}:{}@localhost:3306/kaggle_torrent'
                       '?charset=utf8mb4'.format(mysql_username, mysql_password), pool_recycle=3600)

# Query the database
with engine.connect() as connection:
    # Prepare the query
    columns = [users.c.UserName + '/' + kernels.c.CurrentUrlSlug, kernels.c.TotalVotes, kernels.c.TotalViews]
    s = select(columns)
    s = s.select_from(users.join(kernels).join(kernelVersions).join(kernelLanguages))
    s = s.where(
        and_(
            kernels.c.Medal == 3,
            # users.c.PerformanceTier == 5,
            kernelLanguages.c.Name == 'IPython Notebook'
        )
    )
    s = s.order_by(desc(kernels.c.TotalVotes), desc(kernels.c.TotalViews))

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
