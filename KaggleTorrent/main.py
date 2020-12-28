#!/usr/bin/env python3

import argparse

from KaggleTorrent import config, \
    build_db_schema, \
    populate_db, \
    http_downloader
from KaggleTorrent.db_connection_handler import DbConnectionHandler


def main():
    """This is the entrypoint for this CLI application.
    Here we will handle possible arguments and orchestrate function/method calls
    to build and populate the KaggleTorrent database"""

    # Create the parser
    my_parser = argparse.ArgumentParser(
        prog='KaggleTorrent',
        usage='%(prog)s <generate|update> [options]',
        description='Create or update KaggleTorrent'
    )

    # Add the arguments
    my_parser.add_argument('command',
                           type=str,
                           choices=['generate', 'update'],
                           help='Use the `generate` command to create KaggleTorrent from scratch or '
                                'the `update` command to update KaggleTorrent '
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

    if command == 'generate':

        # Initialize the db
        con = db_engine.connect()
        con.execute('DROP DATABASE IF EXISTS kaggle_torrent;')
        con.execute('CREATE DATABASE IF NOT EXISTS kaggle_torrent CHARACTER SET utf8mb4;')

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

        populate_db.set_foreign_keys(
            sqlalchemy_engine=db_engine,
            constraints_file_path=config.constraints_file_path)

        print("Database populated with MetaKaggle data")

        # Download the notebooks and update the db with their local path
        # To get a specific subset of notebooks, query the database by using
        # the db_schema object as needed.
        print("Notebooks download started")
        http_downloader.HTTPDownloader(
            sqlalchemy_engine=db_engine,
            download_config=config.download_conf
        )
        print("Notebooks download completed")

    elif command == 'update':
        print('Update case!')
        print('Strategy:', args.strategy)
        pass


if __name__ == '__main__':
    import time

    start_time = time.time()
    main()
    print("--- %s minutes ---" % ((time.time() - start_time)/60))
