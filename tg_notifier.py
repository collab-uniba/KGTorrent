import os
import sys

import requests


def telegram_bot_sendtext(bot_message):
    bot_token = os.environ['BOT_TOKEN']
    bot_chat_id = os.environ['CHAT_ID']
    send_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?' \
                f'chat_id={bot_chat_id}&' \
                f'parse_mode=Markdown&' \
                f'text={bot_message}'

    response = requests.get(send_text)

    return response.json()


def main(msg):
    telegram_bot_sendtext(msg)


if __name__ == '__main__':
    main(sys.argv[1])
