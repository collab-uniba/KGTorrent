"""
This is the configuration file of KGTorrent.

Here the main variables of the program are set, mostly by reading their values from environment variables.

See :ref:`configuration` for details on the environment variables that must be set to run KGTorrent.
"""

import logging
import os
import time

# MySQL DB configuration
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']
db_name = os.environ['DB_NAME']
db_username = os.environ['MYSQL_USER']
db_password = os.environ['MYSQL_PWD']

# Data paths
meta_kaggle_path = os.environ['METAKAGGLE_PATH']
constraints_file_path = '../data/fk_constraints_data.csv'

# Notebook dataset configuration
nb_archive_path = os.environ['NB_DEST_PATH']
nb_conf = {
    'languages': ['IPython Notebook HTML']
}

# Logging Configuration
log_path = os.environ['LOG_DEST_PATH']
logging.basicConfig(
    filename=os.path.join(log_path, f'{time.time()}.log'),
    filemode='w',
    level=logging.INFO,
    format='[%(levelname)s]\t%(asctime)s - %(message)s'
)
