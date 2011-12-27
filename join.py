#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Join two input streams, left and right.
Left input will be load to memory as hash index.
This tool provide inner join. Join key must be unique but need not be sorted.

"""

import sys
import argparse
import re

def getColumnIndexes(keyColsStr):
    """
    keyColsStr :: str
        like "1,2,3". 0 means all columns.
    return :: [int]
        list of column index.
    """
    return map(int, keyColsStr.split(','))

def getColumnIndexesWithPrefix(keyColsWithPrefix):
    """
    keyColsWithPrefix :: str
        like "l1,l2,r1,r3". 
        "l" menas left, "r" means right. 0 menas all columns.
    return :: [(str, int)]
        list of prefix and column index.

    """
    re1 = re.compile('(\w+)(\d+)')
    def match(col):
        m = re1.match(col)
        if m:
            return (m.group(1), int(m.group(2)))
        else:
            raise IOError("Parse failed: \"%s\" in \"%s\""
                          % (col, keyColsWithPrefix))
    return map(match, keyColsWithPrefix.split(','))

def prefixToIsLeft((prefix, idx)):
    """
    prefix :: str
       This must be 'l' or 'r'.
    idx :: int
       Column index.
    return :: (bool, int)
       Pair of isLeft and column index.

    """
    if prefix == 'l':
        return (True, idx)
    elif prefix == 'r':
        return (False, idx)
    else:
        raise IOError("prefix must be 'l' or 'r' but '%s'" % prefix)

def generateGetOutputRecord(outColumnIndexes):
    """
    Generate getOutputRecord function.

    outColumnIndexes ::  [(bool, int)]
       list of isLeft and column index.
    return :: (tuple(rec), tuple(rec)) -> tuple(rec)
        merge left record and right record to a record.

    """
    def getOutputRecord(leftRecord, rightRecord):
        """
        leftRecord :: tuple(str)
        rightRecord :: tuple(str)
        return :: tuple(str)
            merged record.
            
        """
        ret = []
        for isLeft, idx in outColumnIndexes:
            if isLeft:
                if idx == 0:
                    ret += leftRecord
                else:
                    ret.append(leftRecord[idx - 1])
            else:
                if idx == 0:
                    ret += rightRecord
                else:
                    ret.append(rightRecord[idx - 1])
        return tuple(ret)
    return getOutputRecord

def parseOpt(argStrs):
    """
    argStrs :: [str]
        argument string list.
    return :: 

    """
    parser = argparse.ArgumentParser(
        description="Join two input streams." + 
        " Left input will be load to memory as hash index." + 
        " This tool provide inner join. Join key must be unique but need not be sorted.")
    parser.add_argument('--version', action='version', version='0.1a')
    parser.add_argument('-lk', metavar='COLS', dest='left_key', default='1',
                        help='Left key columns like "1,2,3". (default: 1).')
    parser.add_argument('-rk', metavar='COLS', dest='right_key', default='1',
                        help='Right key columns like "1,2,3". (default: 1).')
    parser.add_argument('-jk', metavar='COLS', dest='join_key', default=None,
                        help='Both -lk and -rk.')
    parser.add_argument('-oc', metavar='COLS', dest='out_columns', default='l0,r0',
                        help='Output columns. prefix "l" means left input, ' + 
                        '"r" means right input. (default: l0,r0)')
    parser.add_argument('-li', metavar='INPUT', dest='left_input', default=None, required=True,
                        type=argparse.FileType('r'),
                        help='Left input stream. (required)')
    parser.add_argument('-ri', metavar='INPUT', dest='right_input', default=sys.stdin,
                        type=argparse.FileType('r'),
                        help='Right input stream. (default: stdin)')
    parser.add_argument("-s", "--separator", metavar='SEP', dest="separator", default=None,
                        help="Record separator (default: spaces).")
    args = parser.parse_args()
    return args

def getKeyIndexes(args):
    """
    args :: argparse.NameSpace
    return ([int], [int])
        Left key indexes and right key indexes.

    """
    g = getColumnIndexes
    if args.join_key is not None:
        l = g(args.join_key)
        r = l
    else:
        l = g(args.left_key)
        r = g(args.right_key)
    if len(l) != len(r):
        raise IOError("key index length does not equal: left %d right %d."
                      % (len(l), len(r)))
    return (l, r)

def recordReader(f, separator=None):
    """
    f :: file
       Input file.
    separator :: str
       Column separator.
    return :: generator(tuple(str))

    """
    for line in f:
        line = line.rstrip()
        yield tuple(line.split(separator))

def generateGetKeyFromRecord(keyIdxList):
    """
    Generate a function that gets key from record.

    keyIdxList :: [int]
        1 means first column.
        0 means all columns.
    return :: tuple(str) -> tuple(str)
        record -> key.

    """
    def getKeyFromRecord(rec):
        """
        rec :: tuple(str)
        return :: tuple(str)

        """
        ret = []
        for idx in keyIdxList:
            if idx == 0:
                ret += list(rec)
            else:
                ret.append(rec[idx - 1])
        return tuple(ret)
    return getKeyFromRecord

def createHashTable(recordReader, getKeyFromRecord):
    """
    recordReader :: generator(tuple(str))
    getKeyFromRecord :: tuple(str) -> tuple(str)
    return :: dict(tuple(str), tuple(str))
        Map of key and record.

    """
    h = {}
    for rec in recordReader:
        key = getKeyFromRecord(rec)
        if key in h:
            raise IOError("Duplicated key: (%s)" % ','.join(key))
        else:
            h[key] = rec
    return h

def hashJoin(hashTable, recordReader, getKeyFromRecord):
    """
    hashTable :: dict(tuple(str), tuple(str))
       Map of key and value.
    recordReader :: generator(tuple(str))
       Input record reader.
    getKeyFromRecord :: tuple(str) -> tuple(str)
       Get key from record.
    return :: generator((tuple(rec), tuple(rec)))
       Generator of Pair of record in hash table and one from reader.

    """
    for rec in recordReader:
        key = getKeyFromRecord(rec)
        if key in hashTable:
            yield (hashTable[key], rec)

def printStrList(strList, f=sys.stdout):
    """
    Print string list separated by tab.
    
    strList :: [str]

    """
    isNotFirst = False
    for str in strList:
        if isNotFirst:
            print >>f, "\t", str,
        else:
            print >>f, str,
            isNotFirst = True

def doMain():
    args = parseOpt(sys.argv)

    outColumnIdxes = map(prefixToIsLeft, getColumnIndexesWithPrefix(args.out_columns))
    getOutRec = generateGetOutputRecord(outColumnIdxes)
    
    lReader = recordReader(args.left_input, args.separator)
    rReader = recordReader(args.right_input, args.separator)

    lKeyIdxL, rKeyIdxL = getKeyIndexes(args)
    lGetKey = generateGetKeyFromRecord(lKeyIdxL)
    rGetKey = generateGetKeyFromRecord(rKeyIdxL)

    hashTable = createHashTable(lReader, lGetKey)
    resIter = hashJoin(hashTable, rReader, rGetKey)
    
    for lRec, rRec in resIter:
        printStrList(getOutRec(lRec, rRec))
        print

if __name__ == "__main__":
    try:
        doMain()
    except IOError, e:
        print >>sys.stderr, e
