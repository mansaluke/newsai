from os import makedirs
from os.path import exists, isdir
from .nlogger import Log


log = Log(__name__)


def mkdir_p(path):
    import errno
    try:
        path = path
        makedirs(path)
        return path
    except OSError as exc:
        if exc.errno == errno.EEXIST and isdir(path):
            return path
        else:
            raise


def create_path(path):
    if not exists(path):
        a = True
        while a is True:
            user_input = input(
                path +
                ' does not exist. Create folder? (y/n): '
            ).casefold()
            if user_input == 'y':
                path = mkdir_p(path)
            elif user_input == 'n':
                pass
            a = False
    else:
        pass
    return path
