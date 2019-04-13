#!/usr/bin/env python

import getopt
import os
import sys
from powa_collector import PowaCollector, getVersion


def usage(rc):
    """
    Show tool usage
    """
    print("""Usage:
  %s [ -? | --help ] [ -V | --version ]

    -? | --help     Show this message and exits
    -v | --version  Report powa-collector version and exits

See https://powa.readthedocs.io/en/latest/powa-collector/ for
more information about this tool.
""" % os.path.basename(__file__))

    sys.exit(rc)


def main():
    """
    Simple wrapper around PowaCollector
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "?V", ["help", "version"])
    except getopt.GetoptError as e:
        print(str(e))
        usage(1)

    for o, a in opts:
        if o in ("-?", "--help"):
            usage(0)
        elif o in ("-V", "--version"):
            print("%s version %s" % (os.path.basename(__file__), getVersion()))
            sys.exit(0)
        else:
            assert False, "unhandled option"

    app = PowaCollector()
    app.main()


if (__name__ == "__main__"):
    main()
