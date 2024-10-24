import sys
from contextlib import contextmanager

@contextmanager
def redirect_stdout_to_stderr():
    old_stdout = sys.stdout
    old_stdout.flush()
    sys.stdout = sys.stderr
    yield
    sys.stdout = old_stdout

_RESET = '\033[0m'
_RED = '\033[31m'
_GREEN = '\033[32m'

def warning(msg):
    with redirect_stdout_to_stderr():
        print(_RED + msg + _RESET)

def success(msg):
    print(_GREEN + msg + _RESET)

def info(msg):
    with redirect_stdout_to_stderr():
        print(msg)