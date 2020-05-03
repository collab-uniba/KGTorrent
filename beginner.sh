python tg_notifier.py "Beginner: download started." \
&& \
python API_caller_from_file.py \
"./slug_files/Notebooks by beginners - slugs.csv" \
-o "/mnt/ext_hdd/local_repositories/kaggle_notebooks/beginner" \
&& \
python tg_notifier.py "Beginner: download completed."