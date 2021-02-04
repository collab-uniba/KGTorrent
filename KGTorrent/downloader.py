"""
This module handles the actual download of Jupyter notebooks from Kaggle.
"""

import logging
import time
from pathlib import Path
from tqdm import tqdm

import requests
from kaggle.api.kaggle_api_extended import KaggleApi

# Imports for testing
import KGTorrent.config as config
from KGTorrent.db_communication_handler import DbCommunicationHandler


class Downloader:
    """
    The ``Downloader`` class handles the download of Jupyter notebooks from Kaggle.
    First, it queries the database to get notebook slugs and identifiers and then it requests
    notebooks from Kaggle using one of the following two strategies:

    ``HTTP``
        to download full notebooks via HTTP requests;

    ``API``
        to download notebooks via calls to the official Kaggle API;
        generally, Jupyter notebooks downloaded with this strategy miss the output of code cells.

    Notebooks that are already present in the download folder are skipped.
    During the ``refresh`` procedure, notebooks that are already present in the download folder
    but that are no longer referenced in the KGTorrent database are deleted.

    Args:
        sqlalchemy_engine: the SQLAlchemy engine used to connect to the KGTorrent database.
        download_config: the download configuration, set up in :py:mod:`KGTorrent.config`.
        strategy: the download strategy (``HTTP`` or ``API``).
    """

    def __init__(self, kernels_identifiers, nb_archive_path):

        # Notebook slugs and identifiers [UserName, CurrentUrlSlug, CurrentKernelVersionId]
        self.kernels_identifiers = kernels_identifiers

        # Detination Folder
        self.nb_archive_path = nb_archive_path

        # Counters for successes and failures
        self.n_successful_downloads = 0
        self.n_failed_downloads = 0

    def check_destination_folder(self):
        # CHECK EXISTING NOTEBOOKS IN FOLDER

        # Get notebook names
        notebook_paths = list(Path(self.nb_archive_path).glob('*.ipynb'))

        for path in notebook_paths:
            name = path.stem
            split = name.split('_')

            # check if the file have valid name
            if len(split) == 2:

                # If the file exists in folder drop it from res
                if (split[0] in self.kernels_identifiers['UserName'].values) & \
                        (split[1] in self.kernels_identifiers['CurrentUrlSlug'].values):
                    print('Notebook ', name, ' already downloaded')
                    self.kernels_identifiers = self.kernels_identifiers.loc[~(
                            (self.kernels_identifiers['UserName'] == split[0]) &
                            (self.kernels_identifiers['CurrentUrlSlug'] == split[1]))]
                else:  # remove the notebook
                    print('Removing notebook', name, ' not found in db')
                    path.unlink()

            else:  # remove the notebook
                print('Removing notebook', name, ' not valid')
                path.unlink()

    def http_download(self):

        self.n_successful_downloads = 0
        self.n_failed_downloads = 0

        for row in tqdm(self.kernels_identifiers.itertuples(), total=self.kernels_identifiers.shape[0]):

            # Generate URL
            url = 'https://www.kaggle.com/kernels/scriptcontent/{}/download'.format(row[3])
            # Download notebook content to memory
            try:
                notebook = requests.get(url, allow_redirects=True, timeout=5)

            except requests.exceptions.HTTPError as http_err:
                logging.exception(f'HTTPError while requesting the notebook at: "{url}"')
                self.n_failed_downloads += 1
                continue

            except Exception as err:
                logging.exception(f'An error occurred while requesting the notebook at: "{url}"')
                self.n_failed_downloads += 1
                continue

            # Write notebook in folder
            download_path = self.nb_archive_path + f'/{row[1]}_{row[2]}.ipynb'
            with open(Path(download_path), 'wb') as notebook_file:
                notebook_file.write(notebook.content)

            self.n_successful_downloads += 1
            logging.info(f'Downloaded {row[1]}/{row[2]} (ID: {row[3]})')

            # Wait a bit to avoid a potential IP banning
            time.sleep(1)

    def api_download(self):
        # Inizialization and authentication
        # It's need kaggle.json token in ~/.kaggle
        api = KaggleApi()
        api.authenticate()

        self.n_successful_downloads = 0
        self.n_failed_downloads = 0

        for row in tqdm(self.kernels_identifiers.itertuples(), total=self.kernels_identifiers.shape[0]):

            try:
                api.kernels_pull(f'{row[1]}/{row[2]}', path=Path(self.nb_archive_path))

                # Kaggle API save notebook only with slug name
                # Rename downloaded notebook to username/slug
                nb = Path(self.nb_archive_path + f'/{row[2]}.ipynb')
                nb.rename(self.nb_archive_path + f'/{row[1]}_{row[2]}.ipynb')

            except Exception as err:
                logging.exception(f'An error occurred while requesting the notebook {row[1]}/{row[2]}')
                self.n_failed_downloads += 1
                continue

            self.n_successful_downloads += 1
            logging.info(f'Downloaded {row[1]}/{row[2]} (ID: {row[3]})')

            # Wait a bit to avoid a potential IP banning
            time.sleep(1)

    def download_kernels(self, strategy='HTTP'):
        # DOWNLOAD PROCEDURE

        self.check_destination_folder()

        # Number of notebooks to download
        total_rows = self.kernels_identifiers.shape[0]
        print("Total number of notebooks to download:", total_rows)

        # Wait a bit to ensure the print before tqdm bar
        time.sleep(1)

        # HTTP STRATEGY
        if strategy is 'HTTP':
            self.http_download()

        # API STRATEGY
        if strategy is 'API':
            self.api_download()

        # Print download session summary
        # Print summary to stdout
        print("Total number of notebooks to download was:", total_rows)
        print("\tNumber of successful downloads:", self.n_successful_downloads)
        print("\tNumber of failed downloads:", self.n_failed_downloads)

        # Print summary to log file
        logging.info('DOWNLOAD COMPLETED.\n'
                     f'Total attempts: {total_rows}:\n'
                     f'\t- {self.n_successful_downloads} successful;\n'
                     f'\t- {self.n_failed_downloads} failed.')


if __name__ == '__main__':

    print(f"## Connecting to {config.db_name} db on port {config.db_port} as user {config.db_username}")
    db_engine = DbCommunicationHandler(config.db_username,
                                       config.db_password,
                                       config.db_host,
                                       config.db_port,
                                       config.db_name)

    print("** QUERING KERNELS TO DOWNLOAD **")
    kernels_ids = db_engine.get_kernels_identifiers(config.download_conf['languages'])

    downloader = Downloader(kernels_ids.head(), config.nb_archive_path)
    strategies = 'HTTP', 'API'

    print("*******************************")
    print("** NOTEBOOK DOWNLOAD STARTED **")
    print("*******************************")
    print(f'# Selected strategy. {strategies[0]}')
    downloader.download_kernels(strategy=strategies[0])
    print('## Download finished.')
    print(f'# Selected strategy. {strategies[1]}')
    downloader.download_kernels(strategy=strategies[1])
    print('## Download finished.')
