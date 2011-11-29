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
    parser.set_usage("Usage: %prog [options]\n" + \
                     "Sort csv-like records from stdin.")
    parser.add_option("-g", "--groups", dest="group_indexes", default='0',
                      help="Target group indexes separated by comma." + \
                      "You can add a prefix to each index." + \
                      "n or f: the column is treated as float value." + \
                      "i: the column is treated as int value. Example: 2,n3,i1")
    parser.add_option("-k", "--key_func", dest="key_func", default="lambda *args: args",
                      help="Key function. (default: lambda *args: args)")
    parser.add_option("-c", "--cmp_func", dest="cmp_func", default=None,
                      help="(Not supported yet) Compare function. -c is prior to -k option.")
    parser.add_option("-r", "--reverse", action="store_true", dest="reverse", default=False,
                      help="Reverse the result of comprations.")
    parser.add_option("-l", "--load", dest="load_file", default=None,
                      help="Load python code for key_func or cmp_func.")
    parser.add_option("-s", "--separator", dest="separator", default=None,
                      help="Record separator (default: spaces).")
    (options, args2) = parser.parse_args(args)
    return (options, args2)


def keyLineGenerator(idxL, keyFunc, lineG, separator=None):
    """
    Make a (key, line) generator from a line generator.
    
    idxL :: [(str -> ANY, int)]
        Index list.
        1st: converter from string to some type which is comparable.
        2nd: item index.
    keyFunc :: *tuple(ANY) -> ANY
        Key generator. ANY type must be comparable.
    lineG :: generator(str)
        Line generator. (file object etc)
    separator :: str
        Line separator.

    return :: generator((ANY, str))
        1st: sort key which must be comparable.
        2nd: line without eol.
    
    """
    getInputKey = generateInputKeyFunc(idxL)

    def getKeyLine(idxL, keyFunc, line):
        """
        idxL :: [(str -> ANY, int)]
        keyFunc :: *tuple(any) -> tuple(any)
        line :: str
            input string line with eol.
            
        return :: (ANY, str)
            1st: key for sort which must be comparable.
            2nd: line string without eol.
        
        """
        line = line.rstrip()
        lineL = line.split(separator)
        inputKeys = getInputKey(*lineL)
        key = keyFunc(*inputKeys)
        return key, line

    for line in lineG:
        yield getKeyLine(idxL, keyFunc, line)


def generateIdxL(groupIndexesStr):
    """
    groupIndexesStr :: str
        Group index string like "1,3,n2,i5".
    return :: [((str -> ANY),int)]
        1st: converter.
        2nd: index.
        
    """
    indexStrL = groupIndexesStr.split(',')
    re1 = re.compile(r'([fin]?)([0-9]+)')
    def conv(x):
        """
        x :: str
            String must be match '[fin]?[0-9]+' regex.

        return :: (str -> ANY, int)
            1st: i -> converter to int.
                 f or n -> converter to float.
            2nd: index.
        
        """
        m = re1.match(x)
        if m:
            c = m.group(1)
            if c == 'i':
                converter = lambda x : int(x)
            elif c == 'f' or c == 'n':
                converter = lambda x : float(x)
            else:
                converter = lambda x : x
            index = int(m.group(2))
            return (converter, index)
        else:
            raise Exception("%s is wrong as an index." % x)
        
    idxL = list(itertools.imap(conv, iter(indexStrL)))
    return idxL


def generateInputKeyFunc(idxL):
    """
    Generate getInputKey function.

    idxL :: [(str -> ANY), int]
    return :: *tuple(str) -> tuple(ANY)
    
    """
    def getInputKey(*args):
        """
        args :: tuple(str)
        return :: tuple(ANY)

        Constraints:
            len(args) == len(idxL) must be true.
            
        """
        return tuple([converter(args[idx-1]) for converter, idx in idxL])
    
    return getInputKey


def loadFile(fileName):
    """
    Load file as python code.
    
    fileName :: str
    return :: None
    
    """
    fn = fileName
    if fn and os.path.isfile(fn):
        exec(file(fn), globals(), locals())


def generateKeyFunc(keyFuncStr):
    """
    Generate key function.

    keyFuncStr :: str
    return :: *tuple(ANY) -> ANY
        ANY must be comparable.
    
    """
    if keyFuncStr is not None:
        keyFunc = eval(keyFuncStr, globals(), locals())
    else:
        keyFunc = lambda *args: args
    return keyFunc


def main():
    options, args = parseOpts(sys.argv[1:])
    #print options, args

    idxL = generateIdxL(options.group_indexes)
    #print idxL #debug

    loadFile(options.load_file)
    keyFunc = generateKeyFunc(options.key_func)

    for key, line in sorted(keyLineGenerator(idxL, keyFunc, sys.stdin, options.separator),
                            key=lambda (x,y):x,
                            reverse=options.reverse):
        print line


if __name__ == "__main__":
    main()
