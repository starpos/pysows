#!/usr/bin/python

"""
GroupBy aggregator of a list of csv-like record as an input stream.

"""

import sys
import argparse
from decimal import Decimal
import re
import pysows
from util import verify_type, unzip


class Accumulator(object):
    '''
    Base class or acumulator.
    '''
    def __init__(self):
        pass

    def add(self, s):
        raise RuntimeError('Not Implemented')

    def get(self):
        raise RuntimeError('Not Implemented')


class AccSum(Accumulator):
    '''
    sum accumulator.
    '''
    def __init__(self):
        self.v = Decimal('0.0')

    def add(self, s):
        verify_type(s, str)
        self.v += Decimal(s)

    def get(self):
        return self.v


class AccAvg(Accumulator):
    '''
    average accumulator.
    '''
    def __init__(self):
        self.c = 0
        self.v = Decimal('0.0')

    def add(self, s):
        verify_type(s, str)
        self.c += 1
        self.v += Decimal(s)

    def get(self):
        return self.v / Decimal(self.c)


class AccMin(Accumulator):
    '''
    minimum accumulator.
    '''
    def __init__(self):
        self.v = None

    def add(self, s):
        verify_type(s, str)
        v = Decimal(s)
        if self.v is None:
            self.v = v
        elif self.v > v:
            self.v = v

    def get(self):
        return self.v


class AccMax(Accumulator):
    '''
    Maximum accumulator.
    '''
    def __init__(self):
        self.v = None

    def add(self, s):
        verify_type(s, str)
        v = Decimal(s)
        if self.v is None:
            self.v = v
        elif self.v < v:
            self.v = v

    def get(self):
        return self.v


def parseAcc(s):
    '''
    s :: str - like 'avg(2)', 'sum(3)' column index is 1-origin.
    return :: (int, Accumulator generator) - column index (0-origin) and accumulator generator.
    '''
    verify_type(s, str)
    m = re.match('(\w+)\((\d+)\)', s)
    if not m:
        raise RuntimeError('parse operator failed:', s)
    op = m.group(1)
    idx1 = int(m.group(2))
    if idx1 < 1:
        raise RuntimeError('bad index', op, idx1)
    d = {'avg': AccAvg, 'sum': AccSum, 'min': AccMin, 'max': AccMax, }
    if op not in d:
        raise RuntimeError('bad operator', op, idx1)
    return (idx1 - 1, d[op])


class AccumulatorGroup(object):
    '''
    Accumulator for each key.
    '''
    def __init__(self, args):
        verify_type(args, argparse.Namespace)
        self.grpIdxL = map(lambda x: int(x) - 1, args.groupIndexes.split(','))
        self.valIdxL, self.accGenL = unzip(map(parseAcc, args.valueIndexes.split(',')))
        self.hashMap = {}

    def add(self, rec):
        verify_type(rec, list, str)

        key = tuple(self._getSubRecord(rec, self.grpIdxL))
        if key not in self.hashMap:
            self.hashMap[key] = [x() for x in self.accGenL]

        valL = self._getSubRecord(rec, self.valIdxL)
        accL = self.hashMap[key]
        for acc, val in zip(accL, valL):
            acc.add(val)

    def iteritems(self):
        for key, accL in sorted(self.hashMap.iteritems(), key=lambda x: x[0]):
            yield list(key) + [acc.get() for acc in accL]

    def _getSubRecord(self, rec, idxes):
        '''
        rec :: [str] - record
        idxes :: [int] - index list
        return :: [str] - sub record
        '''
        verify_type(rec, list, str)
        verify_type(idxes, list, [int, long])
        ret = []
        for i in idxes:
            ret.append(rec[i])
        return ret


def parseOpts(args):
    '''
    args :: [str] - argument string list
    return :: argparse.Namespace
    '''
    p = argparse.ArgumentParser(description="")
    p.add_argument("-g", "--groups", dest="groupIndexes",
                   metavar='COLUMNS', default='1',
                   help="Column index list for group separated by comma.")
    p.add_argument("-v", "--values", dest="valueIndexes",
                   metavar='COLUMNS', default='avg(2)',
                   help=("List of operator and column index for target separated by comma, \n" +
                         "like avg(2),min(2),max(2). Operator is one of 'avg', 'sum', 'min' or 'max'."))
    p.add_argument("-s", "--separator", dest="separator",
                   metavar='SEP', default=None,
                   help="Record separator (default: spaces).")
    return p.parse_args(args)


def doMain():
    args = parseOpts(sys.argv[1:])
    accGrp = AccumulatorGroup(args)
    for line in sys.stdin:
        rec = line.rstrip().split(args.separator)
        accGrp.add(rec)
    for sL in accGrp.iteritems():
        pysows.printList(sL)
        print


if __name__ == "__main__":
    try:
        doMain()
    except Exception, e:
        pysows.exitWithError(e)
