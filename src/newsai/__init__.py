import os
from pathlib import Path
import pkg_resources
from newsai.utils.nlogger import Log

_ROOT_PATH = os.path.abspath(Path(__file__))  # .parents[2])
_DATA_PATH = os.path.join(_ROOT_PATH, 'data')
