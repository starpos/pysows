#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Pysows utility functions.

"""

import sys
import os
import traceback
import decimal
import re

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


GROUPS_HELP_MESSAGE = \
    "Column index list separated by comma." + \
    " You can add a prefix to each index: " + \
    " 'n' or 'f': a column is treated as float value." + \
    " 'i': a column is treated as int value." + \
    " 'd': a column is treated as Decimal value." + \
    " No prefix: a column is treaded as string." + \
    " Index 1 means the first column and 0 means all columns." + \
    " Example: '2,n3,i1'. (default: '0')"

def getTypedColumnIndexList(typedColumnIndexListStr):
    """
    typedColumnIndexListStr :: str
        Column index list as a string like "1,3,n2,i5".
        Supported prefix is 'i', 'n', 'f', and 'd'.
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

    idxStrL = typedColumnIndexListStr.split(',')
    return map(getConverterAndIndex, idxStrL)

def projectConv(convIdxL, rec):
    """
    Project a record.

    convIdxL :: [(str -> ANY), int]
        Column index list with converter.
        index 1 means first column.
              0 means all columns.
    rec :: tuple(any)
        Input record.
    return :: tuple(any)
        Output projected record.
    throws IOError

    """
    length = len(rec)
    ret = []
    for conv, idx in convIdxL:
        if idx == 0:
            ret += list(rec)
        elif 0 < idx and idx <= length:
            ret.append(conv(rec[idx - 1]))
        else:
            raise IOError("Record length %d but you accesses %d.",
                          length, idx)
    return tuple(ret)

def generateProject(idxL):
    """
    Generate a function that projects a record.

    idxL :: [int]
        column index list.
    return :: tuple(str) -> tuple(str) throws IOError.
        record -> projected record.

    """
    assert(isinstance(idxL, list))
    convIdxL = map(lambda idx: (lambda x: x, idx), idxL)
    return generateProjectConv(convIdxL)

def generateProjectConv(convIdxL):
    """
    Generate a function that projects a record with type conversion.

    convIdxL :: [(str -> ANY), int]
        list of converter and column index.
        index 1 means first column.
        index 0 means all columns (converter will be ignored).
    return :: tuple(str) -> tuple(str) throws IOError
        record -> projected record.

    """
    assert(isinstance(convIdxL, list))
    def project1(rec):
        """
        rec :: tuple(str)
        return :: tuple(str)

        """
        return projectConv(convIdxL, rec)
    return project1

def testGenerateProject():
    project = generateProject([1,2])
    assert project(['a', 'b', 'c', 'd', 'e']) == ('a','b')
    project = generateProject([2,4])
    assert project(['a', 'b', 'c', 'd', 'e']) == ('b','d')
    project = generateProject([0])
    assert project(['a', 'b', 'c', 'd', 'e']) == ('a','b','c','d','e')

    project = generateProjectConv([(int, 1)])
    assert project(['1']) == (1,)
    project = generateProjectConv([(float, 1), (int, 2)])
    assert project(['1.0', '2']) == (1.0, 2)


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
