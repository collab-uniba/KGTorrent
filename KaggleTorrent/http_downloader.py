"""
This module does.
"""

import os
import time

import pandas as pd
import requests
from sqlalchemy import select, desc, and_

from tqdm import tqdm
import logging

import KaggleTorrent.config as config

# TODO: remove those imports after testing
from KaggleTorrent.db_connection_handler import DbConnectionHandler

class HTTPDownloader:

    def __init__(self, sqlalchemy_engine, download_config):

        self.engine = sqlalchemy_engine
        self.lang = download_config['languages'][0]
        self.nb_min_lines = download_config['min_nb_lines']

        # # QUERY PROCEDURE

        # Prepare the query
        query = 'SELECT ' \
                'users.UserName, ' \
                'kernels.CurrentUrlSlug, ' \
                'kernels.CurrentKernelVersionId ' \
                'FROM ' \
                '((kernels INNER JOIN users ON kernels.AuthorUserId = users.Id) ' \
                'INNER JOIN kernelversions ON kernels.CurrentKernelVersionId = kernelversions.Id) ' \
                'INNER JOIN kernellanguages ON kernelversions.ScriptLanguageId = kernellanguages.Id ' \
                f'WHERE kernelversions.TotalLines >= {self.nb_min_lines} '

        # Check for languages
        languages = download_config['languages']
        for lang in languages:

            if lang is languages[0]:
                query = query + f'AND kernellanguages.name LIKE \'{lang}\' '

            else:
                query = query + f'OR kernellanguages.name LIKE \'{lang}\' '

        # Close the query
        query = query + ';'

        # Execute the query
        res = pd.read_sql(sql=query, con=self.engine)

        # # DOWNLOAD HTTP PROCEDURE

        # Counters for successes and failures
        n_successful_downloads = 0
        n_failed_downloads = 0

        # Number of notebooks to download
        total_rows = res.shape[0]
        print("Total number of notebooks to download:", total_rows)

        # Wait a bit to ensure the print before tqdm bar
        time.sleep(1)

        for row in tqdm(res.itertuples(), total=total_rows):
            # Generate URL
            url = 'https://www.kaggle.com/kernels/scriptcontent/{}/download'.format(row[3])
            # print('Downloading full script from ', url, ' ...')
            # print('kernels.c.TotalVotes: {}'.format(row[4]))
            # print('kernels.c.TotalViews: {}'.format(row[5]))

            # Download notebook content to memory
            try:
                notebook = requests.get(url, allow_redirects=True, timeout=5)
            except requests.exceptions.HTTPError as http_err:
                logging.exception(f'HTTPError while requesting the notebook at: "{url}"')
                n_failed_downloads += 1
                continue
            except Exception as err:
                logging.exception(f'An error occurred while requesting the notebook at: "{url}"')
                n_failed_downloads += 1
                continue

            # Write notebook content to file
            download_path = config.nb_archive_path + f'/{row[1]}_{row[2]}.ipynb'
            with open(download_path, 'wb') as notebook_file:
                notebook_file.write(notebook.content)

            n_successful_downloads += 1
            logging.info(f'Downloaded {row[2]}')

            # Wait a bit to avoid a potential IP banning
            time.sleep(1)

        # Print download session summary
        # Print summary to stdout
        print("Total number of notebooks to download was:", total_rows)
        print("\tNumber of successful downloads:", n_successful_downloads)
        print("\tNumber of failed downloads:", n_failed_downloads)

        # Print summary to log file
        logging.info('DOWNLOAD COMPLETED.\n'
                     f'Total attempts: {total_rows}:\n'
                     f'\t- {n_successful_downloads} successful;\n'
                     f'\t- {n_failed_downloads} failed.')


if __name__ == '__main__':

    # Create DB engine
    db_connection_handler = DbConnectionHandler()
    e = db_connection_handler.create_sqlalchemy_engine()

    HTTPDownloader(e, download_config=config.download_conf)
