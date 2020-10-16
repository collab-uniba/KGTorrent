import argparse
import logging
import os
import sys
import time
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.rest import ApiException
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument('slug_file',
                    help='Path to the file containing the Kaggle.com slugs.',
                    type=str)
parser.add_argument('-o', '--output_folder',
                    help='The path to the destination folder, where pulled kernels are supposed to be saved.',
                    type=str,
                    default='./notebooks/')

args = parser.parse_args()

URL_FILE_PATH = args.slug_file
DEST_FOLDER = args.output_folder
Path(DEST_FOLDER).mkdir(parents=True, exist_ok=True)

# Kaggle API Authentication
api = KaggleApi()
api.authenticate()

# Logging configuration
logging.basicConfig(
    filename=os.path.join(DEST_FOLDER, 'log.txt'),
    filemode='w',
    level=logging.INFO,
    format='[%(levelname)s]\t%(asctime)s - %(message)s'
)

successful = 0
failed = 0

with open(URL_FILE_PATH, 'r') as f:
    slugs = f.read().splitlines()
    slugs_pbar = tqdm(slugs, desc='Progress')

    start_time = time.time()

    for slug in slugs_pbar:
        logging.info(f'Pulling: "{slug}"')
        try:
            api.kernels_pull(slug, DEST_FOLDER)
            successful += 1
        except ApiException:
            logging.exception(f'I could not pull "{slug}".', exc_info=False)
            failed += 1
            continue

    end_time = time.time()

consoleHandler = logging.StreamHandler(sys.stdout)
logging.getLogger().addHandler(consoleHandler)
logging.info(f'Download completed in {round(end_time - start_time, 2):,} seconds:\n'
             f'\t- {successful} successful\n'
             f'\t- {failed} failed')
