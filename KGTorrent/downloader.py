"""
This module does.
"""

import time

import pandas as pd
import requests
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi

from tqdm import tqdm
import logging

import KGTorrent.config as config

# TODO: remove those imports after testing
from KGTorrent.db_connection_handler import DbConnectionHandler

class Downloader:
    """

    """

    def __init__(self, sqlalchemy_engine, download_config, strategy='HTTP'):

        self.engine = sqlalchemy_engine
        self.lang = download_config['languages'][0]
        self.nb_min_lines = download_config['min_nb_lines']

        # QUERY PROCEDURE
        print('Getting nokebook slugs from db...')
        # Prepare the query
        query = 'SELECT ' \
                'users.UserName, ' \
                'kernels.CurrentUrlSlug, ' \
                'kernels.CurrentKernelVersionId ' \
                'FROM ' \
                '(((kernels INNER JOIN users ON kernels.AuthorUserId = users.Id) ' \
                'INNER JOIN kernelversions ON kernels.CurrentKernelVersionId = kernelversions.Id) ' \
                'INNER JOIN kernellanguages ON kernelversions.ScriptLanguageId = kernellanguages.Id) ' \
                f'WHERE '

        # Check for languages
        languages = download_config['languages']
        for lang in languages:

            if lang is languages[0]:
                query = query + f'kernellanguages.name LIKE \'{lang}\' '

            else:
                query = query + f'OR kernellanguages.name LIKE \'{lang}\' '

        # Close the query
        query = query + ';'

        # Execute the query
        res = pd.read_sql(sql=query, con=self.engine)

        # CHECK EXISTING NOTEBOOKS IN FOLDER

        # Get notebook names
        notebook_paths = list(Path(config.nb_archive_path).glob('*.ipynb'))

        for path in notebook_paths:
            name = path.stem
            split = name.split('_')

            # check if the file have valid name
            if len(split) == 2:

                # If the file exists in folder drop it from res
                if (split[0] in res['UserName'].values) & (split[1] in res['CurrentUrlSlug'].values):
                    print('Notebook ', name, ' already downloaded')
                    res = res.loc[~((res['UserName'] == split[0]) & (res['CurrentUrlSlug'] == split[1]))]
                else:  # remove the notebook
                    print('Removing notebook', name, ' not found in db')
                    path.unlink()

            else:  # remove the notebook
                print('Removing notebook', name, ' not valid')
                path.unlink()

        # DOWNLOAD PROCEDURE

        # Counters for successes and failures
        n_successful_downloads = 0
        n_failed_downloads = 0

        # Number of notebooks to download
        total_rows = res.shape[0]
        print("Total number of notebooks to download:", total_rows)

        # Wait a bit to ensure the print before tqdm bar
        time.sleep(1)

        # HTTP STRATEGY

        if strategy is 'HTTP':

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
                logging.info(f'Downloaded {row[1]}/{row[2]} (ID: {row[3]})')

                # Wait a bit to avoid a potential IP banning
                time.sleep(1)

        # API STRATEGY

        if strategy is 'API':

            # Inizialization and authentication
            # It's need kaggle.json token in ~/.kaggle
            api = KaggleApi()
            api.authenticate()

            for row in tqdm(res.itertuples(), total=total_rows):

                try:
                    api.kernels_pull(f'{row[1]}/{row[2]}', path=config.nb_archive_path)

                    # Kaggle API save notebook only with slug name
                    # Rename downloaded notebook to username/slug
                    nb = Path(config.nb_archive_path + f'/{row[2]}.ipynb')
                    nb.rename(config.nb_archive_path + f'/{row[1]}_{row[2]}.ipynb')

                except Exception as err:
                    logging.exception(f'An error occurred while requesting the notebook {row[1]}/{row[2]}')
                    n_failed_downloads += 1
                    continue

                n_successful_downloads += 1
                logging.info(f'Downloaded {row[1]}/{row[2]} (ID: {row[3]})')

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

    Downloader(e, download_config=config.download_conf)
