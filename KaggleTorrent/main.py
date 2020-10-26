#!/usr/bin/env python3

from KaggleTorrent import config, populate_db_with_metakaggle_data
from KaggleTorrent.db_connection_handler import DbConnectionHandler


def main():
    """This is the entrypoint for this CLI application.
    Here we will handle possible arguments and orchestrate function/method calls
    to build and populate the KaggleTorrent database"""

    # Create db engine
    db_connection_handler = DbConnectionHandler()
    db_engine = db_connection_handler.create_sqlalchemy_engine()

    # Build the database schema
    # TODO: call build_db_schema()

    # Populate the database with notebook metadata
    populate_db_with_metakaggle_data.populate_db(
        sqlalchemy_engine=db_engine,
        meta_kaggle_path=config.meta_kaggle_path
    )

    # Download the notebooks and update the db with their local path
    # TODO: create a class to handle notebooks download and related db updates


if __name__ == '__main__':
    main()
