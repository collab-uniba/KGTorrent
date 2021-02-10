"""
This module defines the class that handles the actual download of Jupyter notebooks from Kaggle.
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
    It needs the notebook slugs and identifiers ``pandas.DataFrame`` in order to request
    notebooks from Kaggle.
    To do so it uses one of the following two strategies:

    ``HTTP``
        to download full notebooks via HTTP requests;

    ``API``
        to download notebooks via calls to the official Kaggle API;
        Jupyter notebooks downloaded by using this strategy always miss the output of code cells.

    Notebooks that are already present in the download folder are skipped.
    During the ``refresh`` procedure all those notebooks that are already present in the download folder
    but are no longer referenced in the KGTorrent database are deleted.
    """

    def __init__(self, nb_identifiers, nb_archive_path):
        """
        The constructor of this class sets notebook identifiers and download folder provided by the arguments.
        It also initializes the counters for successes and failures.

        Args:
            nb_identifiers: The ``pandas.DataFrame`` containing notebook slugs and identifiers.
            nb_archive_path: The path to the download folder.
        """

        # Notebook slugs and identifiers [UserName, CurrentUrlSlug, CurrentKernelVersionId]
        self.nb_identifiers = nb_identifiers

        # Destination Folder
        self.nb_archive_path = nb_archive_path

        # Counters for successes and failures
        self.n_successful_downloads = 0
        self.n_failed_downloads = 0

    def _check_destination_folder(self):
        """
        This method verifies the bond between notebooks in the download folder and the identifiers
        in the notebook slugs and identifiers ``pandas.DataFrame``.
        It checks whether an identifier of the notebooks, which are present in the destination folder,
        is present in the notebook slugs and identifiers ``pandas.DataFrame``.
        If present it deletes the notebook identifiers from the notebook slugs and identifiers ``pandas.DataFrame``.
        If not present it deletes the bondless notebook in the download folder.
        """

        # Get notebook names
        notebook_paths = list(Path(self.nb_archive_path).glob('*.ipynb'))

        for path in notebook_paths:
            name = path.stem
            split = name.split('_')

            # check if the file have valid name
            if len(split) == 2:

                # If the file exists in folder drop it from res
                if (split[0] in self.nb_identifiers['UserName'].values) & \
                        (split[1] in self.nb_identifiers['CurrentUrlSlug'].values):
                    print('Notebook ', name, ' already downloaded')
                    self.nb_identifiers = self.nb_identifiers.loc[~(
                            (self.nb_identifiers['UserName'] == split[0]) &
                            (self.nb_identifiers['CurrentUrlSlug'] == split[1]))]
                else:  # remove the notebook
                    print('Removing notebook', name, ' not found in db')
                    path.unlink()

            else:  # remove the notebook
                print('Removing notebook', name, ' not valid')
                path.unlink()

    def _http_download(self):
        """
        This method implements the HTTP download strategy.
        """
        self.n_successful_downloads = 0
        self.n_failed_downloads = 0

        for row in tqdm(self.nb_identifiers.itertuples(), total=self.nb_identifiers.shape[0]):

            # Generate URL
            url = 'https://www.kaggle.com/kernels/scriptcontent/{}/download'.format(row[3])

            # Download notebook content to memory
            # noinspection PyBroadException
            try:
                notebook = requests.get(url, allow_redirects=True, timeout=5)

            except requests.exceptions.HTTPError:
                logging.exception(f'HTTPError while requesting the notebook at: "{url}"')
                self.n_failed_downloads += 1
                continue

            except Exception:
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

    def _api_download(self):
        """
        This method implements the API download strategy.
        """

        # Initialization and authentication
        # It's need kaggle.json token in ~/.kaggle
        api = KaggleApi()
        api.authenticate()

        self.n_successful_downloads = 0
        self.n_failed_downloads = 0

        for row in tqdm(self.nb_identifiers.itertuples(), total=self.nb_identifiers.shape[0]):

            # noinspection PyBroadException
            try:
                api.kernels_pull(f'{row[1]}/{row[2]}', path=Path(self.nb_archive_path))

                # Kaggle API save notebook only with slug name
                # Rename downloaded notebook to username/slug
                nb = Path(self.nb_archive_path + f'/{row[2]}.ipynb')
                nb.rename(self.nb_archive_path + f'/{row[1]}_{row[2]}.ipynb')

            except Exception:
                logging.exception(f'An error occurred while requesting the notebook {row[1]}/{row[2]}')
                self.n_failed_downloads += 1
                continue

            self.n_successful_downloads += 1
            logging.info(f'Downloaded {row[1]}/{row[2]} (ID: {row[3]})')

            # Wait a bit to avoid a potential IP banning
            time.sleep(1)

    def download_notebooks(self, strategy='HTTP'):
        """
        This method executes the download procedure using the provided strategy after checking the destination folder.

        Args:
            strategy:  The download strategy (``HTTP`` or ``API``). By default it is ``HTTP``.
        """

        self._check_destination_folder()

        # Number of notebooks to download
        total_rows = self.nb_identifiers.shape[0]
        print("Total number of notebooks to download:", total_rows)

        # Wait a bit to ensure the print before tqdm bar
        time.sleep(1)

        # HTTP STRATEGY
        if strategy is 'HTTP':
            self._http_download()

        # API STRATEGY
        if strategy is 'API':
            self._api_download()

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
    kernels_ids = db_engine.get_nb_identifiers(config.nb_conf['languages'])

    downloader = Downloader(kernels_ids.head(), config.nb_archive_path)
    strategies = 'HTTP', 'API'

    print("*******************************")
    print("** NOTEBOOK DOWNLOAD STARTED **")
    print("*******************************")
    print(f'# Selected strategy. {strategies[0]}')
    downloader.download_notebooks(strategy=strategies[0])
    print('## Download finished.')
