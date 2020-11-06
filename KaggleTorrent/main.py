#!/usr/bin/env python3

from KaggleTorrent import config, \
    build_db_schema, \
    populate_db_with_metakaggle_data
from KaggleTorrent.db_connection_handler import DbConnectionHandler


def main():
    """This is the entrypoint for this CLI application.
    Here we will handle possible arguments and orchestrate function/method calls
    to build and populate the KaggleTorrent database"""

    # Create db engine
    db_connection_handler = DbConnectionHandler()
    db_engine = db_connection_handler.create_sqlalchemy_engine()
    print("Connected to the database")

    # Build the database schema
    db_schema = build_db_schema.DbSchema(
        sqlalchemy_engine=db_engine
    )
    print("Database schema built")

    # Populate the database with notebook metadata
    mk_preprocessor = populate_db_with_metakaggle_data.MetaKagglePreprocessor(
        config.constraints_file_path,
        meta_kaggle_path=config.meta_kaggle_path,
        sqlalchemy_engine=db_engine)
    populate_db_with_metakaggle_data.populate_db(
        mk_preprocessor,
        meta_kaggle_path=config.meta_kaggle_path)
    print("Database populated with MetaKaggle data")

    # Download the notebooks and update the db with their local path
    # To get a specific subset of notebooks, query the database by using
    # the db_schema object as needed.
    print("Notebooks download started")
    # TODO: create a class to handle notebooks download and related db updates
    print("Notebooks download completed")


if __name__ == '__main__':
    import time

    start_time = time.time()
    main()
    print("--- %s minutes ---" % ((time.time() - start_time)/60))
