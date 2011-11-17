#!/usr/bin/env python

import sys
from optparse import OptionParser

def getOperators(opStr, valIdx):
    """
    @opStr str: operator string
    @return (a, a -> [str] -> a, a -> [b]) : operator functions.
    
    """
    def sum_init(valIdx):
        """ @return [float] """
        return map(lambda x: 0.0, range(len(valIdx)))
    def sum_op(a, b):
        """
        a [float]
        b [str]
        
        """
        assert(len(a) == len(b))
        return map(lambda (x,y): x + float(y), zip(a,b))
    def sum_end(a):
        return a
    def avg_init(valIdx):
        return (0, sum_init(valIdx))
    def avg_op(a, b):
        """ a (int, [float])
            b [float] """
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
    
    def __init__(self, options):
        """
        @options Values
        @return 
            
        """
        self.grpIdx = map(lambda x: int(x) - 1, options.groupIndexes.split(','))
        self.valIdx = map(lambda x: int(x) - 1, options.valueIndexes.split(','))
        getInitValues, self.operator, self.merge = getOperators(options.operator, self.valIdx)
        self.initValues = getInitValues(self.valIdx)

    def getKey(self, rec):
        # @rec [str]
        # @return str
        a = ""
        for s in self.getGroup(rec):
            a += s
        return a
       
    def getGroup(self, rec):
        """ @rec [str], @return [str] """
        return self.getSubRecord(rec, self.grpIdx)

    def getValues(self, rec):
        """ @rec [str], @return [str] """
        return self.getSubRecord(rec, self.valIdx)

    def getSubRecord(self, rec, idxes):
        """
        @rec [str]: record
        @rec [int]: index list
        @return [str]: sub record
        
        """
        ret = []
        for i in idxes:
            ret.append(rec[i])
        return ret
    
def parseOpts(args):
    """
    @args [str]: argument string list
    @return (Values, [str])
    
    """
    parser = OptionParser()
    parser.add_option("-g", "--groups", dest="groupIndexes", default='1',
                      help="Group indexes separated by comma.")
    parser.add_option("-v", "--values", dest="valueIndexes", default='2',
                      help="Value indexes separated by comma.")
    parser.add_option("-o", "--op", dest="operator", default='avg',
                      help="Operator. avg or sum.")
    (options, args2) = parser.parse_args(args)
    return (options, args2)

if __name__ == "__main__":
    grpDict = {}
    valDict = {}
    options, args = parseOpts(sys.argv[1:])
    op = Operators(options)
    for line in sys.stdin:
        rec = line.rstrip().split()
        key = op.getKey(rec)
        grps = op.getGroup(rec)
        vals = op.getValues(rec)
        if not grpDict.has_key(key):
            grpDict[key] = grps
            valDict[key] = op.initValues
        valDict[key] = op.operator(valDict[key], vals)
    for key,grps in grpDict.items():
        for g in grps:
            print g, '\t',
        vals = op.merge(valDict[key])
        for v in vals:
            print v, '\t',
        print
