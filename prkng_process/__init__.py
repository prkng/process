import os


__version__ = '1.2'

# get global config
spath = os.environ["PRKNG_SETTINGS"]
CONFIG = {}
execfile(spath, {}, CONFIG)
