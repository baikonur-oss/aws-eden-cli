import sys

from . import cmdline

if __name__ == '__main__':
    sys.exit(cmdline.main(sys.argv[1:]))
