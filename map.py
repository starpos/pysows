#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Map a function to the list of record from an input stream.

"""

import sys
import argparse
import pysows

def parseOpts(argStrList):
    """
    argStrList :: [str]
        argument string list
    return :: argparse.Namespace
    
    """
    parser = argparse.ArgumentParser(
        description="Map a function to a list of record from an input stream.")
    pysows.setVersion(parser)
    parser.add_argument('-g', '--groups', metavar='COLUMNS',
                        dest='group_indexes', default='1',
                        help=pysows.GROUPS_HELP_MESSAGE)
    parser.add_argument('-f', '--mapfunc', metavar='FUNCTION',
                        dest='map_func', default='lambda *xs:xs',
                        help='Map function as python code.' + \
                            ' This must return sequence of printable objects.' + \
                            " (default: 'lambda *xs:xs')")
    parser.add_argument('-c', '--constructor', metavar='FUNCTION',
                        dest='record_constructor',
                        default='lambda xs,ys:list(xs)+list(ys)',
                        help='Record constructor.' + \
                            ' This must return sequence of printable objects.' + \
                            " (default: 'lambda xs,ys:list(xs)+list(ys)')")
    parser.add_argument('-l', '--load', metavar='FILE', dest='load_file', 
                        default=None, help='Load python code for -f and -c.')
    parser.add_argument("-s", "--separator", metavar='SEP',
                        dest="separator", default=None,
                        help="Record separator (default: spaces).")

    return parser.parse_args(argStrList)

def doMain():
    args = parseOpts(sys.argv[1:])

    g = globals()
    l = locals()
    pysows.loadPythonCodeFile(args.load_file, g, l)
    mapFunc = eval(args.map_func, g, l)
    constructor = eval(args.record_constructor, g, l)

    convIdxL = pysows.getTypedColumnIndexList(args.group_indexes)
    assert len(convIdxL) > 0
    getKeyFromRec = pysows.generateProjectConv(convIdxL)

    reader = pysows.recordReader(sys.stdin, args.separator)

    for rec in reader:
        key = getKeyFromRec(rec)
        mapped = mapFunc(*key)
        outRec = constructor(rec, mapped)
        pysows.printList(outRec)
        print

if __name__ == "__main__":
    try:
        doMain()
    except Exception, e:
        pysows.exitWithError(e)
