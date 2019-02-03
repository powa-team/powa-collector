#!/usr/bin/env python

from powa_collector import PowaCollector
import logging

# app = PowaCollector()
app = PowaCollector(loglevel=logging.DEBUG)
app.main()
