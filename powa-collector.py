#!/usr/bin/env python

import sys
from powa_collector import PowaCollector

if (len(sys.argv) > 1):
    print("""Usage:
    powa-collector.py

    See https://powa.readthedocs.io/en/latest/powa-collector/ for
    more information about this tool.
""")
    sys.exit(1)

app = PowaCollector()
app.main()
