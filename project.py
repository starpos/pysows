#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import argparse
import pysows

def parseOpts(args):
    """
    args :: [str]
        Argument string list
    return :: argparse.Namespace

    """
    parser = argparse.ArgumentParser(
        description="Project a record list as an input stream.")
    pysows.setVersion(parser)
    parser.add_argument("-g", "--groups", dest="group_indexes",
                        metavar='COLUMNS', default='0',
                        help="Column index list separated by comma. (default: 0)")
    parser.add_argument("-s", "--separator", dest="separator",
                        metavar='SEP', default=None,
                        help="Record separator. (default spaces)")
    return parser.parse_args(args)

def doMain():
    args = parseOpts(sys.argv[1:])
    idxL = pysows.getTypedColumnIndexList(args.group_indexes)

    for rec in pysows.recordReader(sys.stdin, args.separator):
        pysows.printList(pysows.projectConv(idxL, rec))
        print

if __name__ == "__main__":
    try:
        doMain()
    except Exception, e:
        pysows.exitWithError(e)
