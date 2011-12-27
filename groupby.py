#!/usr/bin/env python

"""
GroupBy aggregator of a list of csv-like record as an input stream.

"""

import sys
import argparse
import pysows

def getOperators(opStr, valIdx):
    """
    opStr :: str
        operator string
    return :: (a, a -> [str] -> a, a -> [b])
        operator functions.
    
    """
    def sum_init(valIdx):
        """ return :: [float] """
        return map(lambda x: 0.0, range(len(valIdx)))
    def sum_op(a, b):
        """
        a :: [float]
        b :: [str]
        
        """
        assert(len(a) == len(b))
        return map(lambda (x,y): x + float(y), zip(a,b))
    def sum_end(a):
        return a
    def avg_init(valIdx):
        return (0, sum_init(valIdx))
    def avg_op(a, b):
        """
        a :: (int, [float])
        b [float]
        
        """
        assert(len(a[1]) == len(b))
        return (a[0] + 1, sum_op(a[1], b))
    def avg_end(a):
        n, xs = a
        return map(lambda x: x / float(n), xs)

    if opStr == "sum":
        return (sum_init, sum_op, sum_end)
    elif opStr == "avg":
        return (avg_init, avg_op, avg_end)
    else:
        return None

class Operators:
    
    def __init__(self, args):
        """
        args :: argparse.Namespace
            
        """
        self.grpIdx = map(lambda x: int(x) - 1, args.groupIndexes.split(','))
        self.valIdx = map(lambda x: int(x) - 1, args.valueIndexes.split(','))
        getInitValues, self.operator, self.merge = getOperators(args.operator, self.valIdx)
        self.initValues = getInitValues(self.valIdx)

    def getKey(self, rec):
        """
        rec :: [str]
        return :: [str]

        """
        return tuple(self.getGroup(rec))
       
    def getGroup(self, rec):
        """
        rec :: [str]
        return :: [str]
        
        """
        return self.getSubRecord(rec, self.grpIdx)

    def getValues(self, rec):
        """
        rec :: [str]
        return :: [str]
        """
        return self.getSubRecord(rec, self.valIdx)

    def getSubRecord(self, rec, idxes):
        """
        rec :: [str]
            record
        idxes :: [int]
            index list
        return :: [str]
            sub record
        
        """
        ret = []
        for i in idxes:
            ret.append(rec[i])
        return ret
    
def parseOpts(args):
    """
    args :: [str]
        argument string list
    return :: argparse.Namespace
    
    """
    parser = argparse.ArgumentParser(
        description="")
    parser.add_argument("-g", "--groups", dest="groupIndexes",
                        metavar='COLUMNS', default='1',
                        help="Column index list for group separated by comma.")
    parser.add_argument("-v", "--values", dest="valueIndexes",
                        metavar='COLUMNS', default='2',
                        help="Column index list for target separated by comma.")
    parser.add_argument("-o", "--op", dest="operator", 
                        metavar='OP', default='avg',
                        help="Operator. 'avg' or 'sum'.")
    parser.add_argument("-s", "--separator", dest="separator",
                        metavar='SEP', default=None,
                        help="Record separator (default: spaces).")
    return parser.parse_args(args)

def doMain():
    grpValMap = {}
    args = parseOpts(sys.argv[1:])
    op = Operators(args)
    for line in sys.stdin:
        rec = line.rstrip().split(args.separator)
        grps = tuple(op.getGroup(rec))
        vals = op.getValues(rec)
        if grps not in grpValMap:
            grpValMap[grps] = op.initValues
        grpValMap[grps] = op.operator(grpValMap[grps], vals)
    for grps, vals in grpValMap.iteritems():
        pysows.printList(list(grps) + op.merge(vals))
        print

if __name__ == "__main__":
    try:
        doMain()
    except Exception, e:
        pysows.exitWithError(e)
