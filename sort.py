#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Sort a list of record as an input stream.

"""

import sys
import re
import argparse
import decimal
import pysows

def parseOpts(args):
    """
    args :: [str]
        argument string list
    return :: arpgarser.Namespace
    
    """
    parser = argparse.ArgumentParser(
        description="Sort a list of record as an input stream.")
    pysows.setVersion(parser)
    parser.add_argument("-g", "--groups", dest="group_indexes", 
                        metavar='COLUMNS', default='0',
                        help="Column index list separated by comma." + \
                            " You can add a prefix to each index: " + \
                            " 'n' or 'f': a column is treated as float value." + \
                            " 'i': a column is treated as int value." + \
                            " No prefix: a column is treaded as string." + \
                            " Index 1 means the first column and 0 means all columns." + \
                            " Example: '2,n3,i1'. (default: '0')")
    parser.add_argument("-k", "--keyfunc", dest="key_func",
                        metavar='FUNCTION', default="lambda *xs: xs",
                        help="Key function. (default: 'lambda *args: args')")
    parser.add_argument("-c", "--cmpfunc", dest="cmp_func", 
                        metavar='FUNCTION', default=None,
                        help="(Not supported yet) Compare function. -c is prior to -k option.")
    parser.add_argument("-r", "--reverse", action="store_true", dest="reverse",
                        default=False,
                        help="Reverse the result of comprations.")
    parser.add_argument("-l", "--load", dest="load_file", default=None,
                        help="Load python code for -k or -c.")
    parser.add_argument("-s", "--separator", dest="separator", 
                        metavar='SEP', default=None,
                        help="Column separator. (default: spaces)")
    return  parser.parse_args(args)

def sortKeyAndLineGenerator(convIdxL, keyFunc, lineG, separator=None):
    """
    Make a (key, line) generator from a line generator.
    
    convIdxL :: [(str -> ANY, int)]
        Index list.
        1st: converter from string to some type which is comparable.
        2nd: item index.
    keyFunc :: *tuple(ANY) -> ANY
        Key generator. ANY type must be comparable.
    lineG :: generator(str)
        Line generator. (file object etc)
    separator :: str
        Column separator.

    return :: generator((ANY, str))
        1st: sort key which must be comparable.
        2nd: line without eol.
    
    """
    def getKey(rec):
        """
        rec :: tuple(str)
        return :: tuple(ANY)

        """
        length = len(rec)
        ret = []
        for converter, idx in convIdxL:
            if idx == 0:
                ret += map(converter, list(rec))
            elif 0 < idx and idx <= length:
                ret.append(converter(rec[idx - 1]))
            else:
                raise IOError("Index outbound error %d [1,%d]" % (idx, length))
        return tuple(ret)

    def getSortKeyAndLine(keyFunc, line):
        """
        keyFunc :: *tuple(ANY) -> tuple(ANY)
        line :: str
            input string line with eol.
            
        return :: (ANY, str)
            1st: key for sort which must be comparable.
            2nd: line string without eol.
        
        """
        line = line.rstrip()
        rec = line.split(separator)
        key = getKey(rec)
        sortKey = keyFunc(*key)
        return sortKey, line

    for line in lineG:
        yield getSortKeyAndLine(keyFunc, line)


def generateConvIdxL(columnIndexListStr):
    """
    columnIndexListStr :: str
        Column index list as a string like "1,3,n2,i5".
    return :: [((str -> ANY),int)]
        1st: converter.
        2nd: index.
        
    """
    def getConverter(prefix):
        """
        prefix :: str
        return :: str -> ANY

        """
        assert(isinstance(prefix, str))
        if prefix == 'i':
            return lambda x: int(x)
        elif prefix == 'f' or prefix == 'n':
            return lambda x: float(x)
        elif prefix == 'd':
            return lambda x: decimal.Decimal(x)
        else:
            return lambda x : x

    re1 = re.compile(r'([find]?)([0-9]+)')
    def getConverterAndIndex(x):
        """
        x :: str
            Column index with prefix.
        return :: (str -> ANY, int)

        """
        m = re1.match(x)
        if m:
            converter = getConverter(m.group(1))
            idx = int(m.group(2))
            return (converter, idx)
        else:
            raise IOError("%s is wrong as an index." % x)

    idxStrL = columnIndexListStr.split(',')
    return map(getConverterAndIndex, idxStrL)

def generateKeyFunc(keyFuncStr, globalNamespace, localNamespace):
    """
    Generate key function.

    keyFuncStr :: str
    return :: *tuple(ANY) -> ANY
        ANY must be comparable.
    
    """
    if keyFuncStr is not None:
        return eval(keyFuncStr, globalNamespace, localNamespace)
    else:
        return lambda *xs: xs

def doMain():
    args = parseOpts(sys.argv[1:])

    convIdxL = generateConvIdxL(args.group_indexes)
    
    g = globals()
    l = locals()
    pysows.loadPythonCodeFile(args.load_file, g, l)
    keyFunc = generateKeyFunc(args.key_func, g, l)
    reader = sortKeyAndLineGenerator(convIdxL, keyFunc, sys.stdin, args.separator)
    
    for sortKey, line in sorted(reader, key=lambda (x,y):x, reverse=args.reverse):
        print line

if __name__ == "__main__":
    try:
        doMain()
    except Exception, e:
        pysows.exitWithError(e)
