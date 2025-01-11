import sys
from contextlib import contextmanager

@contextmanager
def redirect_stdout_to_stderr():
    old_stdout = sys.stdout
    old_stdout.flush()
    sys.stdout = sys.stderr
    yield
    sys.stdout = old_stdout

_COLORS = {
    "red": '\033[31m',
    "green": '\033[32m',
    "yellow": '\033[33m',
    "blue": '\033[34m',
}
_RESET = '\033[0m'


def print_output(content, color=None, stdout=True):
    if color:
        content = _COLORS[color] + content + _RESET

    if stdout:
        print(content)
    else:
        sys.stderr.write(content + '\n')