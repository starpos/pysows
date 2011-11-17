#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import itertools
import collections
from optparse import OptionParser
import re
import os

def parseOpts(args):
    """
    @args [str]: argument string list
    @return (Values, [str])
    
    """
    parser = OptionParser()
    parser.add_option("-g", "--groups", dest="group_indexes", default='0',
                      help="Target group indexes separated by comma.")
    parser.add_option("-s", "--separator", dest="separator", default=None,
                      help="Record separator (default spaces).")
    (options, args2) = parser.parse_args(args)
    return (options, args2)

def select(idxL, xL):
    """
    idxL :: [int]
        index list.
    xL :: [str]
        string list.

    return :: [str]
        selected list.
    
    """
    ret = []
    for i in idxL:
        assert i >= 0
        if i == 0:
            ret += xL
        else:
            ret.append(xL[i - 1])
    return ret

if __name__ == "__main__":
    options, args = parseOpts(sys.argv[1:])
    #print options, args

    indexStrL = options.group_indexes.split(',')
    idxL = list(itertools.imap(lambda x:int(x), iter(indexStrL)))

    for line in sys.stdin:
        line = line.rstrip()
        for x in select(idxL, line.split(options.separator)):
            print x,
        print
