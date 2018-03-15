#!/usr/bin/env python

from powa_remote import PowaRemote
import logging

#app = PowaRemote()
app = PowaRemote(loglevel=logging.DEBUG)
app.main()
