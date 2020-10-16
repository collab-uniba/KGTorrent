#!/usr/bin/env python

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

#
# This python script downloads full Kaggle notebooks (with retrospective data)
# from a list of URLs.
# It leverages a regularity found in the download URLs that are generated
# when a user clicks the download buttons of the Kaggle GUI.
#
# Download URLs are structured as follows:
# https://www.kaggle.com/kernels/scriptcontent/<CurrentKernelVersionId>/download
#
# The "CurrentKernelVersionId" can be found in the `Kernels.csv` file from
# The meta-kaggle dataset available at https://www.kaggle.com/kaggle/meta-kaggle
#

# VARIABLES

# csv file with notebooks info (slug, url)
URLs_FILE = Path('./urls_files/NotebooksByExperts-fullDownloadURLs.csv')

# dest folder for downloaded notebooks
DEST_FOLDER = Path(os.path.expanduser('/mnt/ext_hdd/local_repositories/kaggle_notebooks/kaggle_experienced_retrospective/'))

# dest folder for log files
LOG_DEST_FOLDER = Path('../logs/')

# dest folder for a .csv file with complete notebooks info (slug, url, download_path)
OUTPUT_DEST_FOLDER = Path('../output_files/')

# Counters for successes and failures
n_successful_downloads = 0
n_failed_downloads = 0

# LOGGING CONFIGURATION
logging.basicConfig(
    filename=os.path.join(LOG_DEST_FOLDER, f'{time.time()}.log'),
    filemode='w',
    level=logging.INFO,
    format='[%(levelname)s]\t%(asctime)s - %(message)s'
)

# LOAD URLs FILE
df = pd.read_csv(URLs_FILE)

total_rows = df.shape[0]
print("Total number of notebooks to download:", total_rows)

# DOWNLOAD PROCEDURE
for row in tqdm(df.itertuples(), total=total_rows):

    # Download notebook content to memory
    try:
        notebook = requests.get(row.url, allow_redirects=True, timeout=5)
    except requests.exceptions.HTTPError as http_err:
        logging.exception(f'HTTPError while requesting the notebook at: "{row.url}"')
        n_failed_downloads += 1
        continue
    except Exception as err:
        logging.exception(f'An error occurred while requesting the notebook at: "{row.url}"')
        n_failed_downloads += 1
        continue

    # Write notebook content to file
    download_path = DEST_FOLDER / (row.slug.rsplit('/', 1)[1] + '.ipynb')
    with open(download_path, 'wb') as notebook_file:
        notebook_file.write(notebook.content)

    # Save the path of the downloaded notebook
    df.loc[row.Index, 'download_path'] = download_path

    n_successful_downloads += 1
    logging.info(f'Downloaded {row.slug}')

    # Wait a bit to avoid a potential IP banning
    time.sleep(3)

# Save df with download_paths to file
df.to_csv(OUTPUT_DEST_FOLDER / f'notebook_download_paths_{time.time()}.csv', index=False, sep=';')

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
