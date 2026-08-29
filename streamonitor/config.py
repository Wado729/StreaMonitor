import json
import sys
import time

from streamonitor.bot import Bot
from streamonitor.log import Logger

logger = Logger('[CONFIG]').get_logger()
config_loc = "config.json"


def load_config():
    try:
        with open(config_loc, "r+") as f:
            return json.load(f)
    except FileNotFoundError:
        with open(config_loc, "w+") as f:
            json.dump([], f, indent=4)
            return []
    except Exception as e:
        print(e)
        sys.exit(1)


def save_config(config):
    try:
        with open(config_loc, "w+") as f:
            json.dump(config, f, indent=4)

        return True
    except Exception as e:
        print(e)
        sys.exit(1)


def loadStreamers():
    streamers = []
    for streamer in load_config():
        username = streamer["username"]
        site = streamer["site"]

        bot_class = Bot.str2site(site)
        if not bot_class:
            logger.warning(f'Unknown site: {site} (user: {username})')
            continue

        try:
            streamer_bot = bot_class.fromConfig(streamer)
        except Exception as e:
            logger.warning(
                f'Failed to load streamer {username} on {site}: {e!r} - skipping'
            )
            continue
        streamers.append(streamer_bot)
        streamer_bot.start()
        time.sleep(0.1)
    return streamers
