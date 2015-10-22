import os

from prkng_process.logger import set_level


__version__ = '1.2'

# get global config
spath = os.environ["PRKNG_SETTINGS"]
CONFIG = {}
execfile(spath, {}, CONFIG)

set_level(CONFIG["LOG_LEVEL"])
