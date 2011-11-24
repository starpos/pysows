#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import itertools
import collections
from optparse import OptionParser
import os
import re

def parseOpts(args):
    """
    @args [str]: argument string list
    @return (Values, [str])
    
    """
    parser = OptionParser()
    parser.add_option("-g", "--groups", dest="group_indexes", default='0',
                      help="Target group indexes separated by comma.")
    parser.add_option("-p", "--predicate", dest="predicate", default='lambda *x:True',
                      help="Predicate as python code.\n" + \
                      "This must return bool value.")
    parser.add_option("-r", "--regex", dest="regex", default=None,
                      help="Regular expression.\n" + \
                      "Both -p and -r options are specified, -r is used.")
    parser.add_option("-l", "--load", dest="load_file", default=None,
                      help="Load python code for map_func.")
    parser.add_option("-s", "--separator", dest="separator", default=None,
                      help="Record separator (default spaces).")
    parser.add_option("-v", "--invert", action="store_true", dest="invert", default=False,
                      help="Invert match like grep -v.")
    (options, args2) = parser.parse_args(args)
    return (options, args2)


def select(idxL, xL):
    """
    idxL :: [int]
        Index list.
        Specify 1 for first element.
        0 means all elements.
    xL :: [str]
        Target string list.
    return :: [str]
        Selected string list.
    
    """
    tmpL = []
    for i in idxL:
        assert i >= 0
        if i == 0:
            tmpL += xL
        else:
            tmpL.append(xL[i - 1])
    return tmpL
    

def generateFilterByPredicate(predicateStr, idxL, separator=None):
    """
    predicateStr :: str
        Predicate string.
        After evaled, predicate type must be (*xs -> bool).
    idxL :: [int]
        Index list.
        Specify 1 for first element.
        0 means all elements.
    separator :: str
        Separator string of text line.
    return :: str -> bool
        Filter function.
        
    """
    predicate = eval(predicateStr, globals(), locals())

    def getTargetStrL(line):
        """
        line :: str
        return :: [str]
        
        """
        return select(idxL, line.split(separator))

    def filterByPredicate(line):
        """
        line :: str
        return :: bool
        
        """
        return predicate(*tuple(getTargetStrL(line)))

    return filterByPredicate


def generateFilterByRegex(regexStr, idx, separator=None):
    """
    regexStr :: str
        Regular expression string.
    idx :: int
        Target index.
        Specify 1 for first element.
        0 means whole line.
    separator :: str
        Separator string of text line.
    return :: str -> bool
        Filter function.
    
    """
    assert idx >= 0
    regex = re.compile(options.regex)
    if idx == 0: # whole line.
        def getTargetStr(line):
            """
            line :: str
            return :: str
            
            """
            return line
    else:
        def getTargetStr(line):
            """
            line :: str
            return :: str

            """
            strL = line.split(separator)
            return strL[idx - 1]
        
    def filterByRegex(line):
        """
        line :: str
        return :: bool
        
        """
        return regex.match(getTargetStr(line)) is not None

    return filterByRegex
    

if __name__ == "__main__":
    options, args = parseOpts(sys.argv[1:])
    #print options, args

    indexStrL = options.group_indexes.split(',')
    idxL = list(itertools.imap(lambda x:int(x), iter(indexStrL)))

    fn = options.load_file
    if fn and os.path.isfile(fn):
        exec(file(fn), globals(), locals())
    if options.regex is None:
        filterBy = \
            generateFilterByPredicate(options.predicate, idxL, options.separator)
    elif len(idxL) != 1:
        print >>sys.stderr, "-r requires just one argument for -g."
        sys.exit(1)
    else:
        filterBy = \
            generateFilterByRegex(options.regex, idxL[0], options.separator)
    if options.invert:
        notIfInvert = lambda x: not x
    else:
        notIfInvert = lambda x: x
        
    for line in sys.stdin:
        line = line.rstrip()
        if notIfInvert(filterBy(line)):
            print line

