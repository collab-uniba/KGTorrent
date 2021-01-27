#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

from sqlalchemy_utils import database_exists, \
    create_database, \
    drop_database

from KGTorrent import config, \
    build_db_schema, \
    populate_db, \
    downloader
from KGTorrent.db_connection_handler import DbConnectionHandler


def main():
    """This is the entrypoint for this CLI application.
    Here we will handle possible arguments and orchestrate function/method calls
    to build and populate the KGTorrent database"""

    # Create the parser
    my_parser = argparse.ArgumentParser(
        prog='KGTorrent',
        usage='%(prog)s <init|refresh> [options]',
        description='Initialize or refresh KGTorrent'
    )

    # Add the arguments
    my_parser.add_argument('command',
                           type=str,
                           choices=['init', 'refresh'],
                           help='Use the `init` command to create KGTorrent from scratch or '
                                'the `refresh` command to update KGTorrent '
                                'according to the last version of Meta Kaggle.')

    my_parser.add_argument('--strategy',
                           type=str,
                           choices=['API', 'HTTP'],
                           default='HTTP',
                           help="Use the `API` strategy to download Kaggle kernels via the Kaggle's official API; "
                                "Use the `HTTP` strategy to download full kernels via HTTP requests."
                                "N.B.: Notebooks downloaded via the Kaggle API miss code cell outputs.")

    # my_parser.add_argument('--DBstring',
    #                        type=str,
    #                        help="Details about the database connection must be provided as environment variables;"
    #                             "Alternatively, you can specify them using this command line argument."
    #                             "The expected format is: '<usr>:<pwd>@<host>:<port>/<db_name>'")

    # my_parser.add_argument('--dataset_path',
    #                        type=str,
    #                        help="Path of the directory in which the dataset is stored.")

    # Execute the parse_args() method
    args = my_parser.parse_args()

    command = args.command

    # Create db engine
    db_connection_handler = DbConnectionHandler()
    db_engine = db_connection_handler.create_sqlalchemy_engine()
    print("Connected to the database")

    # CHECK USER VARIABLES
    proceed = None

    # Check db emptiness
    if database_exists(db_engine.url):
        if command == 'init':
            print(f'Database {config.db_name} already exists. ', file=sys.stderr)
            print(f'Please, provide a name that is not already in use for the KGTorrent database.',
                  file=sys.stderr)
            proceed = False
        if command == 'refresh':
            print(f'Database {config.db_name} already exists. This operation will reinitialize the current database')
            print('and populate it with the provided MetaKaggle version.')
            ans = input(f'Are you sure to re-initialize {config.db_name} database? [yes]\n')
            if ans.lower() == 'yes':
                proceed = True
            else:
                proceed = False
    else:
        proceed = True

    # Check download folder emptiness when init
    has_data = next(Path(config.nb_archive_path).iterdir(), None)
    if (has_data is not None) & (command == 'init'):
        print(f'Download folder {config.nb_archive_path} is not empty.', file=sys.stderr)
        print('Please, provide the path to an empty folder to store downloaded notebooks.', file=sys.stderr)
        proceed = False

    #KGTorrent process
    if proceed:

        print('Initializing the database...')
        # Initialize the db
        if database_exists(db_engine.url):
            drop_database(db_engine.url)
        create_database(db_engine.url,'utf8mb4')

        print('Building table schemas...')
        # Build the database schema
        db_schema = build_db_schema.DbSchema(
            sqlalchemy_engine=db_engine
        )
        print("Database schema built")

        # Populate the database with notebook metadata
        mk_preprocessor = populate_db.MetaKagglePreprocessor(
            config.constraints_file_path,
            meta_kaggle_path=config.meta_kaggle_path,
            sqlalchemy_engine=db_engine)
        populate_db.populate_db(
            mk_preprocessor)

        print('Applying foreign key constraints')
        populate_db.set_foreign_keys(
            sqlalchemy_engine=db_engine,
            constraints_file_path=config.constraints_file_path)

        print("Database populated with MetaKaggle data")

        # Download the notebooks and update the db with their local path
        # To get a specific subset of notebooks, query the database by using
        # the db_schema object as needed.
        print("Notebooks download started")
        downloader.Downloader(
            sqlalchemy_engine=db_engine,
            download_config=config.download_conf,
            strategy=args.strategy
        )
        print("Notebooks download completed")

    time.sleep(0.2)
    print('KGTorrent end')


if __name__ == '__main__':
    import time

    start_time = time.time()
    main()
    print("--- %s minutes ---" % ((time.time() - start_time)/60))
