#/usr/bin/env python

__all__ = ['u2s', 's2u', 'gplus', 'checkAndThrow', 'unzip', 'unzipG',
           'isList', 'isTuple']

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

def unzip(listOfPair):
  """
  listOfPair :: list(tuple(any))
  return :: (list, list)

  """
  a, b = unzipG(listOfPair)
  return (list(a), list(b))

def unzipG(listOfPair):
  """
  listOfPair :: list(tuple(any))
  return :: generator(

  """
  def g1():
    for a, _ in listOfPair:
      yield a

  def g2():
    for _, b in listOfPair:
      yield b

  return g1(), g2()

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
