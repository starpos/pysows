#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Pysows utility functions.

"""

import sys
import os
import traceback

VERSION_STR = '0.2'

def setVersion(parser):
    """
    parser :: argparser.Parser

    """
    parser.add_argument('--version', action='version', version=VERSION_STR)


def loadPythonCodeFile(fileName, globalNamespace, localNamespace):
    """
    Load python code file.

    fileName :: str
        File name to load as python code.
    globalNamespace :: dict
        Global namespace.
    localNamespace :: dict
        Local namespace.
    
    """
    if fileName and os.path.isfile(fileName):
        exec(file(fileName), globalNamespace, localNamespace)

def getColumnIndexList(columnIndexListStr):
    """
    Convert column index list as a string.

    columnIndexListStr :: str
        like "1,2,3". 0 means all columns.
    return :: [int]
        list of column index.

    """
    return map(int, columnIndexListStr.split(','))

def testGetColumnIndexList():
    assert getColumnIndexList('1,2,3') == [1,2,3]
    assert getColumnIndexList('3,5,1') == [3,5,1]

def project(idxL, rec):
    """
    Project a record.

    idxL :: [int]
        Column index list.
        1 means first column.
        0 means all columns.
    rec :: tuple(any)
        Input record.
    return :: tuple(any)
        Output projected record.
    throws IOError

    """
    length = len(rec)
    ret = []
    for idx in idxL:
        if idx == 0:
            ret += list(rec)
        elif 0 < idx and idx <= length:
            ret.append(rec[idx - 1])
        else:
            raise IOError("Record length %d but you accesses %d.",
                          length, idx)
    return tuple(ret)

def generateProject(idxL):
    """
    Generate a function that projects a record.

    idxL :: [int]
        1 means first column.
        0 means all columns.
    return :: tuple(str) -> tuple(str) throws IOError
        record -> projected record.

    """
    def project1(rec):
        """
        rec :: tuple(str)
        return :: tuple(str)

        """
        return project(idxL, rec)
    return project1

def testGenerateProject():
    project = generateProject([1,2])
    assert project(['a', 'b', 'c', 'd', 'e']) == ('a','b')
    project = generateProject([2,4])
    assert project(['a', 'b', 'c', 'd', 'e']) == ('b','d')
    project = generateProject([0])
    assert project(['a', 'b', 'c', 'd', 'e']) == ('a','b','c','d','e')

def recordReader(f, separator=None):
    """
    Wrapper of file object as an text input stream.

    f :: file
       Input file.
    separator :: str
       Column separator.
    return :: generator(tuple(str))

    """
    for line in f:
        line = line.rstrip()
        yield tuple(line.split(separator))

def testRecordReader():
    def dummyLines():
        for i in range(0,10):
            yield ("%d %s\n" % (i, chr(ord('a') + i)))
    i = 0
    for rec in recordReader(dummyLines()):
        assert len(rec) == 2
        assert rec == (str(i), chr(ord('a') + i))
        i += 1

def printList(anyList, f=sys.stdout, sep='\t'):
    """
    Print list of printable objects.
    
    anyList :: [any]
        any must be printable.
    f :: file
        Output stream.
    sep :: str
        Separator string.

    """
    isNotFirst = False
    for item in anyList:
        if isNotFirst:
            print >>f, sep, item,
        else:
            print >>f, item,
            isNotFirst = True

def exitWithError(e):
    """
    e :: Exception

    """
    print >>sys.stderr, e
    traceback.print_exc()
    sys.exit(1)
