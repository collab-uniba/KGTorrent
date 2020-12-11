# Remember to source all environment variables before executing this script.
# Environment variables should be saved to a .env file inside this folder.
# Source them by issuing the command `source .env`

python ./utils/tg_notifier.py "KT_test: download started." && \
				python -m KaggleTorrent.http_downloader && \
				python ./utils/tg_notifier.py "KT_test: download completed." || \
				python ./utils/tg_notifier.py "KT_test: download failed."
