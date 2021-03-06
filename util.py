#/usr/bin/python
# -*- coding: utf-8 -*-

import re

def u2s(sizeU):
    """
    Convert size with unit suffix to size as integer value.

    """
    assert(isinstance(sizeU, str))
    m = re.match('([0-9]+)([kmgtpKMGTP]){0,1}', sizeU)
    assert(m is not None)
    n = int(m.group(1))
    suffix = m.group(2)
    if suffix is not None:
        suffix = suffix.lower()
    x = 1024 ** [None, 'k', 'm', 'g', 't', 'p'].index(suffix)
    return n * x


def s2u(size):
    """
    Convert size as integer value to string with unit suffix.

    """
    assert(isinstance(size, int))
    c = 0
    while size % 1024 == 0:
        c += 1
        size /= 1024
    return str(size) + ['', 'k', 'm', 'g', 't', 'p'][c]


def gplus(g0, g1):
    """
    Concatinate two iterable objects.

    g0 :: iter(a)
    g1 :: iter(a)
    return :: generator(a)
    a :: any

    """
    for a in g0:
        yield a
    for b in g1:
        yield b


def checkAndThrow(cond, msg=""):
    """
    cond :: bool
    msg :: str
    return :: None
    throw :: AssertionError

    """
    if not cond:
        raise AssertionError(msg)


def unzip(generatorOfTuple):
    '''
    generatorOfTuple :: generator(tuple)
    return :: tuple(list)
    '''
    return tuple(map(list, list(unzipG(generatorOfTuple))))


def unzipG(generatorOfTuple):
    '''
    generatorOfTuple :: generator(tuple)
    return :: tuple(generator)
    '''
    def g1():
        for t in generatorOfTuple:
            yield list(t)
    g = g1()
    try:
        itemL = g.next()
    except StopIteration:
        return ()
    nr = len(itemL)
    LL = [[] for _ in xrange(nr)]
    for L, item in zip(LL, itemL):
        L.append(item)

    class G2(object):
        def __init__(self, i):
            self.i = i
        def __iter__(self):
            return self
        def next(self):
            if len(LL[self.i]) == 0:
                itemL = g.next()
                for L, item in zip(LL, itemL):
                    L.append(item)
            return LL[self.i].pop(0)

    return tuple([G2(i) for i in xrange(nr)])


def isList(objL, cnst):
    """
    objL :: [any]
    cnst :: constructor like int, str.
      for isinstance(obj, cnst)
    return :: bool

    """
    if not isinstance(objL, list):
        return False
    return isSequence(objL, cnst)


def isTuple(objT, cnst):
    if not isinstance(objT, tuple):
        return False
    return isSequence(objT, cnst)


def isSequence(objSeq, cnst):
    """
    objSeq :: any sequence.
    cnst :: constructor.
    return :: bool

    """
    for obj in objSeq:
        if not isinstance(obj, cnst):
            return False
    return True


def verify_type(obj, typeValue, elemType=None):
    '''Verify type of an object.

    It raises a TypeError when none of typeValue(s) did match obj.

    obj       - object.
    typeValue - type like int, str, list. or a list of them.
    elemType  - specify type of elements if typeValue is sequence. or a list of them.
    '''
    if obj is None:
        raise TypeError('None type')
    if not isinstance(typeValue, list):
        typeValue = [typeValue]
    if all([not isinstance(obj, t) for t in typeValue]):
        raise TypeError('Invalid object type: {} must be one of [{}]'
                        .format(str(type(obj)), ','.join([str(t) for t in typeValue])))
    if elemType is None:
        return
    if not isinstance(elemType, list):
        elemType = [elemType]
    for elem in obj:
        if all([not isinstance(elem, t) for t in elemType]):
            raise TypeError('Invalid element type: {} must be one of [{}]'
                            .format(str(type(elem)), ','.join([str(t) for t in elemType])))


def verify_int(obj):
    '''
    obj -- object to verify.
    '''
    if not isinstance(obj, int) and not isinstance(obj, long):
        raise Exception('invalid type', type(obj))
