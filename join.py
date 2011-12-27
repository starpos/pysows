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
import pysows

def getColumnIndexListWithPrefix(keyColsWithPrefix):
    """
    keyColsWithPrefix :: str
        like "l1,l2,r1,r3". 
        "l" menas left, "r" means right. 0 menas all columns.
    return :: [(str, int)]
        list of prefix and column index.

    """
    re1 = re.compile(r'([a-zA-Z_]+)([0-9]+)')
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

def parseOpts(argStrs):
    """
    argStrs :: [str]
        argument string list.
    return :: argparse.Namespace

    """
    parser = argparse.ArgumentParser(
        description="Join two input streams." + 
        " Left input will be load to memory as hash index." + 
        " This tool provide inner join. Join key must be unique but need not be sorted.")
    pysows.setVersion(parser)
    parser.add_argument('-lk', metavar='COLUMNS', dest='left_key', default='1',
                        help='Left key columns like "1,2,3". (default: 1).')
    parser.add_argument('-rk', metavar='COLUMNS', dest='right_key', default='1',
                        help='Right key columns like "1,2,3". (default: 1).')
    parser.add_argument('-jk', metavar='COLUMNS', dest='join_key', default=None,
                        help='Both -lk and -rk.')
    parser.add_argument('-oc', metavar='COLUMNS', dest='out_columns', default='l0,r0',
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
    args = parser.parse_args(argStrs)
    return args

def getKeyIndexLists(args):
    """
    args :: argparse.NameSpace
    return ([int], [int])
        Left key indexes and right key indexes.

    """
    g = pysows.getColumnIndexList
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

def doMain():
    args = parseOpts(sys.argv[1:])

    outColumnIdxes = map(prefixToIsLeft,
                         getColumnIndexListWithPrefix(args.out_columns))
    getOutRec = generateGetOutputRecord(outColumnIdxes)
    
    lReader = pysows.recordReader(args.left_input, args.separator)
    rReader = pysows.recordReader(args.right_input, args.separator)

    lKeyIdxL, rKeyIdxL = getKeyIndexLists(args)
    lGetKey = pysows.generateProject(lKeyIdxL)
    rGetKey = pysows.generateProject(rKeyIdxL)

    hashTable = createHashTable(lReader, lGetKey)
    resultIter = hashJoin(hashTable, rReader, rGetKey)
    
    for lRec, rRec in resultIter:
        pysows.printList(getOutRec(lRec, rRec))
        print

if __name__ == "__main__":
    try:
        doMain()
    except Exception, e:
        pysows.exitWithError(e)
