from sqlalchemy import create_engine

import KGTorrent.config as config


class DbConnectionHandler:

    def __init__(self):
        self.host = config.db_host
        self.port = config.db_port
        self.db_name = config.db_name
        self.mysql_username = config.db_username
        self.mysql_password = config.db_password

    def create_sqlalchemy_engine(self):
        """Creates an SQLAlchemy engine for the MySQL KGTorrent database"""

        engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
            self.mysql_username,
            self.mysql_password,
            self.host,
            self.port,
            self.db_name
        ),
            pool_recycle=3600)

        return engine
