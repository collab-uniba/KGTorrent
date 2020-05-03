#!/bin/bash

python tg_notifier.py "Experienced: download started." \
&& \
python API_caller_from_file.py \
"./slug_files/Notebooks by experts - slugs.csv" \
-o "/mnt/ext_hdd/local_repositories/kaggle_notebooks/experienced" \
&& \
python tg_notifier.py "Experienced: download completed."