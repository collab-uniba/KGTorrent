import os

from sqlalchemy import create_engine, select, desc
from kaggle.api.kaggle_api_extended import KaggleApi

from buildDbSchema import (users, userAchievements, kernels, kernelLanguages, kernelVersions)

# Kaggle API Authentication
api = KaggleApi()
api.authenticate()

mysql_username = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PWD']

# Before executing this script, execute in the mysql client the following command
# CREATE DATABASE IF NOT EXISTS kaggle_torrent CHARACTER SET utf8mb4;

engine = create_engine('mysql+pymysql://{}:{}@localhost:3306/kaggle_torrent'
                       '?charset=utf8mb4'.format(mysql_username, mysql_password), pool_recycle=3600)

with engine.connect() as connection:
    # Query: the most professional notebooks
    columns = [users.c.UserName + '/' + kernels.c.CurrentUrlSlug]
    s = select(columns)
    s = s.select_from(kernels.join(users)).where(kernels.c.AuthorUserId == users.c.Id)
    s = s.where(users.c.PerformanceTier == 5)
    s = s.order_by(desc(kernels.c.TotalVotes), desc(kernels.c.TotalViews))
    s = s.limit(20)
    rp = connection.execute(s)
    # result = rp.fetchall()

DEST_FOLDER = './API_data/'

for r in rp:
    print('Pulling "{}"...'.format(r[0]))
    api.kernels_pull(r[0], DEST_FOLDER)
