#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import itertools
from optparse import OptionParser
import os

def parseOpts(args):
    """
    @args [str]: argument string list
    @return (Values, [str])
    
    """
    parser = OptionParser()
    parser.add_option("-g", "--groups", dest="group_indexes", default=None,
                      help="Target group indexes separated by comma.")
    parser.add_option("-f", "--mapfunc", dest="map_func", default='lambda x:[x]',
                      help="Map function as python code.\n" + \
                      "This must return sequence of printable.")
    parser.add_option("-c", "--constructor", dest="record_constructor", default='lambda x,y:x+list(y)',
                      help="Record constructor.\n" + \
                      "This must return sequence of printable.")
    parser.add_option("-l", "--load", dest="load_file", default=None,
                      help="Load python code for map_func.")
    (options, args2) = parser.parse_args(args)
    return (options, args2)


if __name__ == "__main__":
    options, args = parseOpts(sys.argv[1:])
    #print options, args
    
    fn = options.load_file
    if fn and os.path.isfile(fn):
        exec(file(fn), globals(), locals())

    map_func = eval(options.map_func, globals(), locals())
    record_constructor = eval(options.record_constructor, globals(), locals())

    if options.group_indexes:
        indexStrL = options.group_indexes.split(',')
        idxL = list(itertools.imap(lambda x:int(x), iter(indexStrL)))
    else:
        idxL = []
    #print idxL
    
    def getFuncArgs(strL):
        if len(idxL) == 0:
            return tuple(strL)
        else:
            ret = []
            for i in idxL:
                assert i - 1 >= 0
                assert i - 1 < len(strL) 
                ret.append(strL[i - 1])
            return tuple(ret)

    for line in sys.stdin:
        line = line.rstrip()
        dataL = line.split()
        args = getFuncArgs(dataL)
        mapped = map_func(*args)
        res = record_constructor(dataL, mapped)

        for x in res:
            print x,
        print
