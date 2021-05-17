#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

import KGTorrent.config as config
from KGTorrent.data_loader import DataLoader
from KGTorrent.db_communication_handler import DbCommunicationHandler
from KGTorrent.downloader import Downloader
from KGTorrent.mk_preprocessor import MkPreprocessor


def main():
    """Entry-point function for KGTorrent.
    It orchestrates function/method calls to build and populate the KGTorrent database and dataset."""

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

    # Execute the parse_args() method
    args = my_parser.parse_args()

    command = args.command

    print("************************")
    print("*** KGTORRENT STARTED***")
    print("************************")

    # Create db engine
    print(f"## Connecting to {config.db_name} db on port {config.db_port} as user {config.db_username}")
    db_engine = DbCommunicationHandler(config.db_username,
                                       config.db_password,
                                       config.db_host,
                                       config.db_port,
                                       config.db_name)

    print("## Connection with database established.")

    # CHECK USER VARIABLES
    proceed = None

    # Check db emptiness
    if db_engine.db_exists():
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
    data = next(Path(config.nb_archive_path).iterdir(), None)
    if (data is not None) & (command == 'init'):
        print(f'Download folder {config.nb_archive_path} is not empty.', file=sys.stderr)
        print('Please, provide the path to an empty folder to store downloaded notebooks.', file=sys.stderr)
        proceed = False

    # KGTorrent process
    if proceed:

        print("********************")
        print("*** LOADING DATA ***")
        print("********************")
        dl = DataLoader(config.constraints_file_path, config.meta_kaggle_path)

        print("***********************************")
        print("** TABLES PRE-PROCESSING STARTED **")
        print("***********************************")
        mk = MkPreprocessor(dl.get_tables_dict(), dl.get_constraints_df())
        processed_dict, stats = mk.preprocess_mk()

        print("*************")
        print("*** STATS ***")
        print("*************\n")
        print(stats)

        print("## Initializing DB...")
        db_engine.create_new_db(drop_if_exists=True)

        print("***************************")
        print("** DB POPULATION STARTED **")
        print("***************************")
        db_engine.write_tables(processed_dict)

        print("** APPLICATION OF CONSTRAINTS **")
        db_engine.set_foreign_keys(dl.get_constraints_df())

        print("** QUERYING KERNELS TO DOWNLOAD **")
        nb_identifiers = db_engine.get_nb_identifiers(config.nb_conf['languages'])

        # Free memory
        del dl
        del mk
        del db_engine

        # Download the notebooks and update the db with their local path
        # To get a specific subset of notebooks, query the database by using
        # the db_schema object as needed.
        print("*******************************")
        print("** NOTEBOOK DOWNLOAD STARTED **")
        print("*******************************")
        downloader = Downloader(nb_identifiers, config.nb_archive_path)
        print(f'# Selected strategy. {args.strategy}')
        downloader.download_notebooks(strategy=args.strategy)
        print('## Download finished.')

    time.sleep(0.2)
    print('## KGTorrent end')


if __name__ == '__main__':
    import time

    start_time = time.time()
    main()
    print("--- %s minutes ---" % ((time.time() - start_time)/60))
