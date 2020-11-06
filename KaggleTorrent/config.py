"""
Main configuration file.
Place any further config info here.
"""

import os

# MySQL DB configuration
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']
db_name = os.environ['DB_NAME']
db_username = os.environ['MYSQL_USER']
db_password = os.environ['MYSQL_PWD']

# Paths
meta_kaggle_path = os.environ['METAKAGGLE_PATH']
constraints_file_path = os.environ['CONSTRAINTS_FILE_PATH']
nb_archive_path = ''
