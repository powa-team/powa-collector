Overview
========

This repository contains the `powa-collector` tool, a simple multi-threaded
python program that performs the snapshots for all the remote servers
configured in a powa repository database (in the **powa_servers** table).

Requirements
============

This program requires python 2.7 or python 3.

The required dependencies are listed in the **requirements.txt** file.

Configuration
=============

Copy the provided `powa-collector.conf-dist` file to a new `powa-collector.conf`
file, and adapt the **dsn** specification to be able to connect to the wanted
main PoWA repository.

Usage
=====

To start the program, simply run the powa-collector.py program.  A `SIGTERM` or a
`Keyboard Interrupt` on the program will cleanly stop all the thread and exit
the program.  A `SIGHUP` will reload the configuration.
