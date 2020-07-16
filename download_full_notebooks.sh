#!/bin/bash

python tg_notifier.py "EXPERIENCED: download started." && \
python download_full_kaggle_notebooks.py && \
python tg_notifier.py "EXPERIENCED: download completed." || \
python tg_notifier.py "EXPERIENCED: download failed."