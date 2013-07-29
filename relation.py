#!/usr/bin/env python
# -*- coding: utf-8 -*-

__all__ = ['Schema', 'Record', 'Relation', 'joinRelations']

import collections
import itertools
import decimal
import re

import util


COL_TYPE_DICT = {}

def registerColumnType(typeName, colTypeCls):
  COL_TYPE_DICT[typeName] = colTypeCls

def searchColumnType(typeName):
  if typeName in COL_TYPE_DICT:
    return COL_TYPE_DICT[typeName]
  else:
    return None


class ColumnType:
  """
  ColumnType data.
  This is used to parse string and convert to required-type value.

  """
  @classmethod
  def parse(cls, valStr):
    """
    valStr :: str
    return :: ColumnType-specific

    """
    raise NotImplementedError()

  @classmethod
  def name(cls):
    """
    return :: str

    """
    raise NotImplementedError()

  @classmethod
  def isValid(cls):
    """
    return :: bool

    """
    raise NotImplementedError()

  @classmethod
  def toStr(cls, val):
    """
    Default implementation.

    return :: str

    """
    return str(val)

  def __str__(self):
    return self.name()

  def __eq__(self, rhs):
    return self.name() == rhs.name()


class StringColumnType(ColumnType):

  @classmethod
  def parse(cls, valStr):
    assert(isinstance(valStr, str))
    return valStr

  @classmethod
  def name(cls):
    return "String"

  @classmethod
  def isValid(cls, val):
    return isinstance(val, str)

class IntegerColumnType(ColumnType):

  @classmethod
  def parse(cls, valStr):
    assert(isinstance(valStr, str))
    return int(valStr)

  @classmethod
  def name(cls):
    return "Integer"

  @classmethod
  def isValid(cls, val):
    return isinstance(val, int)

  def __str__(self):
    return self.name()

class FloatColumnType(ColumnType):

  @classmethod
  def parse(cls, valStr):
    assert(isinstance(valStr, str))
    return float(valStr)

  @classmethod
  def name(cls):
    return "Float"

  @classmethod
  def isValid(cls, val):
    return isinstance(val, float)

class DecimalColumnType(ColumnType):

  @classmethod
  def parse(cls, valStr):
    assert(isinstance(valStr, str))
    return decimal.Decimal(valStr)

  @classmethod
  def name(cls):
    return "Decimal"

  @classmethod
  def isValid(cls, val):
    return isinstance(val, decimal.Decimal)

class SchemaEntry:
  """
  Schema entry.
  Use cls.parse() to parge formatted string to create an object.
  Use str(obj) to put formatted string.

  """
  def __init__(self, colName, colType):
    assert(isinstance(colName, str))
    #print "colType", colType
    assert(isinstance(colType, ColumnType))
    self.__colName = colName
    self.__colType = colType

  def valid(self):
    """
    return :: bool
    throw :: AssertionError

    """
    util.checkAndThrow(isinstance(self.name(), str))
    util.checkAndThrow(len(self.name()) > 0)
    util.checkAndThrow(isinstance(self.type(), ColumnType))
    return True

  def name(self):
    return self.__colName

  def rename(self, newName):
    """
    newName :: str

    """
    assert(isinstance(newName, str))
    self.__colName = newName

  def type(self):
    return self.__colType

  def __str__(self):
    return "%s::%s" % (self.name(), self.type().name())

  regex = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)(?:::([a-zA-Z0-9_]+))?')

  @classmethod
  def parse(cls, schemaEntryStr):
    """
    schemaEntryStr :: str
    return :: SchemaEntry
    throw :: TypeError

    """
    assert(isinstance(schemaEntryStr, str))
    m = cls.regex.match(schemaEntryStr)
    if not m:
      raise TypeError("Invalid schema format.")
    name = m.group(1)
    typeName = m.group(2)
    if typeName is None:
      typeName = StringColumnType.name()
    colTypeCls = searchColumnType(typeName)
    if colTypeCls is None:
      raise TypeError("Invalid type %s." % typeName)
    return SchemaEntry(name, colTypeCls())

  def copy(self):
    """
    Copy (type is immutable object).
    return :: SchemaEntry

    """
    return SchemaEntry(str(self.name()), self.type())

  def parseValue(self, valueStr):
    """
    valueStr :: str
    return :: a
      self.type().isValid(a) must be True.

    """
    assert(isinstance(valueStr, str))
    return self.type().parse(valueStr)

class Schema:
  """
  Manage a list of column name and type.

  """
  def __init__(self, schemaEntryL):
    assert(self.check1(schemaEntryL))
    self.__schemaEntryL = schemaEntryL
    self.__nameIdx = {} # key (name :: str), value (idx :: int)

    for entry, idx in zip(schemaEntryL, xrange(len(schemaEntryL))):
      self.__insertNameIdx(entry.name(), idx)

  @classmethod
  def create(cls, colNameL, colTypeL):
    assert(cls.check2(colNameL, colTypeL))
    schemaEntryL = map(lambda (colName, colType):
                       SchemaEntry(colName, colType),
                       zip(colNameL, colTypeL))
    return Schema(schemaEntryL)

  @classmethod
  def check1(cls, schemaEntryL):
    util.checkAndThrow(isinstance(schemaEntryL, list))
    for entry in schemaEntryL:
      util.checkAndThrow(entry is not None)
      entry.valid()
    return True

  @classmethod
  def check2(cls, colNameL, colTypeL):
    util.checkAndThrow(isinstance(colNameL, list))
    util.checkAndThrow(isinstance(colTypeL, list))
    util.checkAndThrow(len(colNameL), len(colTypeL))
    for colName in colNameL:
      util.checkAndThrow(isinstance(colName, str))
    for colType in colTypeL:
      util.checkAndThrow(isinstance(colType, ColumnType))
    return True

  def __getitem__(self, nameOrIdx):
    """
    nameOrIdx :: str | int
    return :: SchemaEntry

    """
    if isinstance(nameOrIdx, int):
      return self.__schemaEntryL[nameOrIdx] # may throw IndexError.
    else:
      assert(isinstance(nameOrIdx, str))
      return self.getEntry(nameOrIdx)

  def getType(self, nameOrIdx):
    """
    return :: ColumnType-inherited | None

    """
    e = self[nameOrIdx]
    if e is None:
      return None
    else:
      return e.type()

  def getIdx(self, name):
    """
    return :: int | None

    """
    assert(isinstance(name, str))
    if name in self.__nameIdx:
      return self.__nameIdx[name]
    else:
      return None

  def getEntry(self, name):
    """
    return :: SchemaEntry | None

    """
    idx = self.getIdx(name)
    if idx is None:
      return None
    else:
      return self[idx]

  def getEntryIdx(self, name):
    """
    name :: str
    return :: (SchemaEntry, int) | None

    """
    idx = self.getIdx(name)
    if idx is None:
      return None
    else:
      return (self[idx], idx)

  def foreach(self):
    """
    return :: generator(SchemaEntry)

    """
    for entry in self.__schemaEntryL:
      yield entry

  def size(self):
    """
    return :: int

    """
    return len(self.__schemaEntryL)

  def show(self, sep='\t'):
    assert(isinstance(sep, str))
    return '#' + sep.join(map(str, self.__schemaEntryL))

  def __str__(self):
    return self.show()

  @classmethod
  def parse(self, schemaStr, sep=None):
    """
    schemaStr :: str
    sep :: separator.
    return :: Schema

    """
    schemaStr = schemaStr.rstrip()
    schemaStr = re.sub('^#', '', schemaStr)
    return Schema(map(SchemaEntry.parse, schemaStr.split(sep)))

  def isMatch(self, rawRec):
    """
    Check that a rawRec matches the schema.

    rawRec :: tuple(any)
    return :: bool

    """
    assert(isinstance(rawRec, tuple))

    if self.size() != len(rawRec):
      return False

    for schemaE, val in zip(self.foreach(), rawRec):
      if not schemaE.type().isValid(val):
        return False

    return True

  def toStr(self, rawRec, sep='\t'):
    """
    Convert to rawRec to string representation.
    rawRec :: tuple
    sep :: str
    return :: str

    """
    assert(self.isMatch(rawRec))
    return sep.join(map(lambda (schemaE, val): schemaE.type().toStr(val),
                        zip(self.foreach(), rawRec)))

  def project(self, colNameL):
    """
    Create new schema from colNameL and
    return index list.

    colNameL :: [T]
      T :: str | int
         name or index.
    return :: (Schema, [int])
    throw :: ValueError | IndexError

    """
    assert(isinstance(colNameL, list))
    eL = []
    iL = []
    for colName in colNameL:
      if isinstance(colName, str):
        ei = self.getEntryIdx(colName)
        if ei is None:
          raise ValueError("There is no column %s." % colName)
        entry, idx = ei
      else:
        assert(isinstance(colName, int))
        idx = colName
        entry = self[idx]
      eL.append(entry)
      iL.append(idx)
    return (Schema(eL), iL)

  def __eq__(self, rhs):
    """
    Equality (except for column name).

    """
    for left, right in zip(self.__schemaEntryL, rhs.__schemaEntryL):
      if left.type() != left.type():
        return False
    return True

  def copy(self):
    """
    return :: Schema

    """
    eL = []
    for entry in self.foreach():
      eL.append(entry.copy())
    return Schema(eL)

  def renameCol(self, oldName, newName):
    """
    Rename a column.
    oldName :: str
    newName :: str
    return :: None
    throw :: ValueError

    """
    idx = self.getIdx(oldName)
    if idx is None:
      raise ValueError("column %s not found." % oldName)
    del self.__nameIdx[oldName]
    self.__insertNameIdx(newName, idx)

    self[idx].rename(newName)

  def __insertNameIdx(self, name, idx):
    """
    return :: None

    """
    assert(isinstance(name, str))
    assert(isinstance(idx, int))
    if name in self.__nameIdx:
      raise TypeError("schema entry name is not unique.")
    self.__nameIdx[name] = idx

  def parseValues(self, valueStrs):
    """
    valueStrs :: [str] | tuple([str])
    return :: tuple([any])
      This must be the same type of rawRec.

    """
    assert(util.isList(valueStrs, str) or util.isTuple(valueStrs, str))
    if util.isTuple(valueStrs, str):
      valueStrs = list(valueStrs)

    assert(self.size() == len(valueStrs))
    return tuple(map(lambda (e, s): e.type().parse(s), zip(self.__schemaEntryL, valueStrs)))

"""
You can define new ColumnType here.

"""

# Register ColumnType-inherited classes.
for cls in [StringColumnType, IntegerColumnType, FloatColumnType, DecimalColumnType]:
  registerColumnType(cls.name(), cls)


def testSchema():
  entry = SchemaEntry.parse('c1')
  print "entry1", entry
  entry = SchemaEntry.parse('c1::Integer')
  print "entry2", entry
  schema = Schema.parse('#c1::Integer c2::Decimal c3::Float c4')
  print "schema", schema
  assert(schema.isMatch((1, decimal.Decimal(1.0), float(1.0), '1.0')))
  print schema.toStr((1, decimal.Decimal(1.0), float(1.0), '1.0'))

class Column:
  """
  Column data.

  """
  def __init__(self, schemaEntry, colVal):
    """
    schemaEntry :: SchemaEntry
    colVal :: any
      schemaEntry.type().isValid(colVal) must be True.

    """
    assert(isinstance(schemaEntry, SchemaEntry))
    assert(colVal is not None)
    assert(schemaEntry.type().isValid(colVal))
    self.__schemaEntry = schemaEntry
    self.__raw = colVal

  @classmethod
  def parse(cls, schemaEntry, colValStr):
    return Column(schemaEntry, schemaEntry.type().parse(colValStr))

  def schemaEntry(self):
    return self.__schemaEntry

  def name(self):
    """
    return :: str

    """
    return self.schemaEntry().name()

  def type(self):
    """
    return :: ColumnType-inherited

    """
    return self.schemaEntry().type()

  def raw(self):
    """
    return :: ColumnType-specific.

    """
    return self.__raw

  def __str__(self):
    """
    return :: str

    """
    return self.type().toStr(self.raw())

def testColumn():
  s = Column.parse(SchemaEntry.parse('name1::String'), '1.1')
  i = Column.parse(SchemaEntry.parse('name2::Integer'), '111')
  f = Column.parse(SchemaEntry.parse('name3::Float'), '1.1')
  d = Column.parse(SchemaEntry.parse('name4::Decimal'), '1.1')
  print s.name(), s.type(), s.raw()
  print i.name(), i.type(), i.raw()
  print f.name(), f.type(), f.raw()
  print d.name(), d.type(), d.raw()


class Record:
  """
  Record value.

  self.__schema :: Schema
  self.__raw :: tuple(any)

  """
  def __init__(self, schema, rawRec):
    """
    schema :: Schema
    rawRec :: tuple(any)
       schema.isMatch(rawRec) must be True.

    """
    assert(isinstance(schema, Schema))
    assert(isinstance(rawRec, tuple))
    assert(schema.isMatch(rawRec))
    self.__schema = schema
    self.__raw = rawRec

  @classmethod
  def create(self, colL):
    """
    colL :: [Column]

    """
    assert(isinstance(colL, list))
    schemaEntryL, rawL = util.unzip(map(lambda col: (col.schemaEntry(), col.raw()), colL))
    return Record(Schema(schemaEntryL), tuple(rawL))

  def raw(self):
    """
    return :: tuple(any)

    """
    return self.__raw

  def schema(self):
    """
    return :: Schema

    """
    return self.__schema

  def show(self):
    """
    return :: str

    """
    return str(self.raw())

  def __str__(self):
    """
    return :: str

    """
    return self.schema().toStr(self.raw())

  def __len__(self):
    return self.size()

  def size(self):
    """
    return :: int

    """
    assert(self.schema().size() == len(self.raw()))
    return len(self.raw())

  def project(self, colNameL):
    """
    colNameL :: [T]
      T :: str | int
    return :: Record

    """
    schema, idxL = self.schema().project(colNameL)
    return Record(schema, self.getRawKey(idxL))

  def __getitem__(self, colNameL):
    """
    colNameL :: T | [T] | tuple([T])
      T :: str | int
    return :: any | tuple([any])

    """
    if isinstance(colNameL, str) or isinstance(colNameL, int):
      return self[[colNameL]][0]
    else:
      assert(isinstance(colNameL, list) or isinstance(colNameL, tuple))
      if isinstance(colNameL, tuple):
        colNameL = list(colNameL)
      return self.project(colNameL).raw()

  def getRawKey(self, idxL):
    """
    idxL :: [int]
    return :: tuple([any])

    """
    return projectRawRec(self.raw(), idxL)

  def __eq__(self, rhs):
    return self.schema() == rhs.schema() and self.raw() == rhs.raw()


"""
Raw record utilities.

"""

def projectRawRec(rawRec, idxL):
  """
  rawRec :: tuple([any])
  idxL :: [int]
  return :: tuple([any])

  """
  assert(isinstance(rawRec, tuple))
  assert(util.isList(idxL, int))
  return tuple(map(lambda idx: rawRec[idx], idxL))


def testRecord():
  """
  For test.

  """
  schema = Schema.parse('#col0 col1 col2')
  rawRec = ('0', '1', '2')
  rec = Record(schema, rawRec)
  assert(rec.size() == 3)
  assert(rec.schema() == schema)
  assert(rec.project(['col0']).raw() == ('0',))
  assert(rec.project(['col1']).raw() == ('1',))
  assert(rec.project(['col2']).raw()  == ('2',))
  assert(rec.project(['col0', 'col1']).raw() == ('0', '1'))
  assert(rec.project(['col2', 'col0']).raw() == ('2', '0'))
  assert(rec.raw() == rawRec)
  assert(rec.project(['col1']) == Record(Schema.parse('#col1'), ('1',)))

  schema = Schema.parse('#c0::Integer c1::Float c2::Decimal')
  rawRec = (0, 0.0, decimal.Decimal(0))
  rec = Record(schema, rawRec)
  assert(len(rec) == 3)
  assert(rec.schema() == Schema.parse('#t0::Integer t1::Float t2::Decimal'))
  assert(rec[0] == 0)
  assert(rec[1] == 0.0)
  assert(rec[2] == decimal.Decimal(0))
  assert(rec['c0'] == 0)
  assert(rec['c1'] == 0.0)
  assert(rec['c2'] == decimal.Decimal(0))

class IterableData:
  """
  A iterable data.

  """

  def __init__(self, iterable, reuse=False):

    self.__isL = isinstance(iterable, list)
    if self.__isL:
      self.__l = iterable
      self.__g = None
    else:
      assert(isinstance(iterable, collections.Iterable))
      self.__l = None
      self.__g = iterable
    self.__reuse = reuse

  def isL(self):
    return self.__isL

  def isG(self):
    return not self.isL()

  def toL(self):
    if not self.__isL:
      self.__isL = True
      self.__l = list(self.__g)
      self.__g = None
    return self.__l

  def __iter__(self):
    if self.__reuse:
      self.toL()
    if self.__isL:
      for x in self.__l:
        yield x
    else:
      for x in self.__g:
        yield x

  def __str__(self):
    return str(self.toL())

  def __add__(self, rhs): # (+)
    assert(isinstance(rhs, IterableData))
    return IterableData(util.gplus(self.__iter__(), rhs.__iter__()),
                        reuse=self.__reuse)


def testIterableData():

  xs = [0, 1, 2, 3, 4]

  ys = IterableData(xs)
  assert(ys.isL())
  assert(list(ys) == xs)

  def g():
    for x in xs:
      yield x

  ys = IterableData(g())
  assert(ys.isG())
  assert(list(ys) == xs)
  for x in ys:
    raise "ys must be empty."
  ys = IterableData(xs) + IterableData(xs)
  assert(ys.__str__() == '[0, 1, 2, 3, 4, 0, 1, 2, 3, 4]')


class Relation:
  """
  Relation type.
    rawRec :: tuple([str])
    rawKey :: tuple([str])

  """
  def __init__(self, schema, iterable, name=None, reuse=False):
    """
    schema :: Schema
    iterable :: iterable(rawRec)
    name :: str
      Relation name.
    reuse :: bool
      True if reuse where all records will be copied.
      Specify False when you will access the records just once.

    """
    assert(isinstance(schema, Schema))

    self.__schema = schema.copy() #copy
    self.__name = str(name) #copy
    self.__reuse = reuse
    self.__idata = IterableData(iterable, reuse=reuse)

  def schema(self):
    return self.__schema

  def renameCol(self, oldCol, newCol):
    """
    Rename a column name.

    oldCol :: str
      old column name.
    newCol :: str
      new column name.

    If oldCol not exists, ValueError wil be thrown.

    """
    assert(isinstance(oldCol, str))
    assert(isinstance(newCol, str))
    self.schema().renameCol(oldCol, newCol)

  def name(self):
    return self.__name

  def getL(self):
    """
    return :: [rawRec]

    """
    return self.__idata.toL()

  def getG(self):
    """
    return :: generator(rawRec)

    """
    return self.__idata

  def getRecG(self):
    """
    return :: generator(Record)

    """
    for rawRec in self.getG():
      yield Record(self.schema(), rawRec)

  def getRecL(self):
    """
    return :: [Record]

    """
    return list(self.getRecG())

  def size(self):
    return len(self.getL())

  def select(self, indexes, reuse=False):
    """
    indexes :: [int]
        each item must be from 0 to num of records - 1.
    return :: Relation

    """
    def gen():
      for idx in indexes:
        yield self.getL()[idx]
    return Relation(self.schema(), gen(), self.__reuse)

  """
  def __getitem__(self, indexes):
    self.select(indexes, reuse=False)
  """

  def filter(self, pred, reuse=False):
    """
    pred :: Record -> bool
    return :: Relation

    """
    def predicate(rawRec):
      return pred(Record(self.schema(), rawRec))
    g = itertools.ifilter(predicate, self.getG())
    return Relation(self.schema(), g, reuse)

  def project(self, cols, reuse=False):
    """
    cols :: [str]
    return :: Relation

    """
    assert(util.isList(cols, str))
    schema, idxL = self.schema().project(cols)

    def mapper(rawRec):
      return projectRawRec(rawRec, idxL)
    g = itertools.imap(mapper, self.getG())
    return Relation(schema, g, reuse)

  def sort(self, cols=None, key=None, lesser=None, reverse=False, reuse=False):
    """
    cols :: [str]
        Name of columns.
    key :: Record -> a
    lesser :: (Record, Record) -> bool

    reverse :: bool
    reuse :: bool

    a :: any
       Comparable each other.

    return :: Relation

    """
    if cols is not None:
      def getKey(rec):
        """
        rec :: Record
        return :: tuple([any])

        """
        assert(isinstance(rec, Record))
        return rec[cols]
      g = sorted(self.getRecG(), key=getKey, reverse=reverse)
    elif key is not None:
      g = sorted(self.getRecG(), key=key, reverse=reverse)
    elif lesser is not None:
      def cmpRec(rec0, rec1):
        assert(isinstance(rec0, Record))
        assert(isinstance(rec1, Record))
        if lesser(rec0, rec1):
          return -1
        elif lesser(rec1, rec0):
          return 1
        else:
          return 0
      g = sorted(self.getRecG(), cmp=cmpRec, reverse=reverse)
    else:
      def getKey2(rec):
        assert(isinstance(rec, Record))
        return rec.raw()
      g = sorted(self.getRecG(), key=getKey2, reverse=reverse)

    return Relation(self.schema(),
                    itertools.imap(lambda rec: rec.raw(), g), reuse=reuse)


  def groupbyAsRelation(self, keyCols, valCols=None, reuse=False):
    """
    keyCols :: [str]
        Name of key columns.
    valCols :: [str] | None
        Name of value columns.

    return :: dict(keyColsT :: tuple([str]), Relation)

    """
    def op(rel, rec):
      assert(isinstance(rel, Relation))
      assert(isinstance(rec, Record))
      rawRec = rec.raw() if valCols is None else rec[valCols]
      rel.insert(rawRec)
      return rel
    def cstr():
      if valCols is None:
        schema = self.schema().copy()
      else:
        schema, _ = self.schema().project(valCols)
      return Relation(schema, [], reuse=reuse)
    return self.groupby(keyCols, op, cstr)

  def groupby(self, keyCols, op, cstr):
    """
    'group by' operation.

    keyCols :: [str]
        Name of key columns.
    op :: (a -> Record -> a)
    cstr :: () -> a
        Constructor for initial value.

    return :: dict(keyColsT :: tuple([str]), a)

    a :: any

    """
    def tmpOp(d, rec):
      assert(isinstance(d, dict))
      assert(isinstance(rec, Record))
      rawKey = rec[keyCols]
      if rawKey not in d:
        d[rawKey] = cstr()
      d[rawKey] = op(d[rawKey], rec)
      return d
    return self.foldl(tmpOp, {})

  def foldl(self, op, init):
    """
    Fold a relation from left side.
    op :: a -> Record -> a
    init :: a
    rel :: Relation
    return :: a

    a :: any

    """
    return reduce(op, self.getRecG(), init)

  def foldr(self, op, init, rel):
    """
    fold a relation from right side.

    """
    raise "Not yet implemented."

  def map(self, colsFrom, schemaTo, mapper, name=None, reuse=False):
    """
    Map a relation to another.

    colsFrom :: [str]
    schemaTo :: Schema
    mapper :: rawRec -> rawRec

    """
    assert(util.isList(colsFrom, str))
    assert(isinstance(schemaTo, Schema))
    def g():
      for rec in self.getRecG():
        yield mapper(rec[colsFrom])
    return Relation(schemaTo, g(), name=name, reuse=reuse)

  def mapR(self, schemaTo, mapper, name=None, reuse=False):
    """
    Map a relation to another.
    You can use schema information inside the mapper func
    by calling record.schema().

    schemaTo :: Schema
        Result schema.
    mapper :: Record -> rawRec

    """
    def g():
      for rec in self.getRecG():
        yield mapper(rec)
    return Relation(schemaTo, g(), name=name, reuse=reuse)

  def mapG(self, mapper, colsFrom=None):
    """
    Convert to Arbitrary-typed lazy list from records.

    mapper :: rawRec -> a
    return :: generator(a) | None
    a :: any

    """
    if colsFrom is None:
      tmpRel = self
    else:
      tmpRel = self.project(colsFrom)
    return itertools.imap(mapper, tmpRel.getG())

  def mapL(self, mapper, colsFrom=None):
    """
    mapper :: rawRec -> a
    colsFrom :: [str] | None
    return :: [a]
    a :: any

    """
    if colsFrom is None:
      tmpRel = self
    else:
      tmpRel = self.project(colsFrom)
    return map(mapper, tmpRel.getL())

  def insert(self, rawRec):
    assert(self.schema().isMatch(rawRec))
    self.getL().append(rawRec)

  def insertRec(self, rec):
    assert(isinstance(rec, Record))
    assert(self.schema() == rec.schema())
    self.getL().append(rec.raw())

  def insertL(self, rawRecL):
    assert(isinstance(rawRecL, list))
    self.__idata = self.__idata + IterableData(rawRecL, reuse=self.__reuse)

  def show(self, sep='\t'):
    """
    sep :: str
    return :: str

    """
    return '\n'.join(list(self.showG(sep=sep))) + '\n'
    #return str(self.schema()) + '\n' + \
    #    '\n'.join(map(lambda rawRec: self.schema().toStr(rawRec), self.getL())) + \
    #    '\n'

  def showG(self, sep='\t'):
    """
    return :: generator(str)

    """
    assert(isinstance(sep, str))
    yield self.schema().show(sep)
    for rawRec in self.getG():
      yield self.schema().toStr(rawRec, sep)

  def __str__(self):
    """
    return :: str

    """
    return self.show()

def testRelation():
  """
  For test.

  """
  schemaStr = '#c1::Integer c2::Integer c3::Integer'
  schema = Schema.parse(schemaStr)
  rawRecL = [(x, y, z) for (x, y) in [(0, 1), (1, 0)] for z in range(0, 5)]
  rel = Relation(schema, rawRecL, reuse=True)
  print rel.show()

  # schema
  assert(rel.schema() == schema)

  # map
  mapper = lambda (c1, c2): (c1 + c2,)
  rel1 = rel.map(['c1', 'c2'], Schema.parse('#c4::Integer'), mapper)
  for rawRec in rel1.getG():
    print rawRec

  print rel1.show()
  assert(all(map(lambda (x,): x == 1, rel1.getL())))

  # foldl
  def op(i, rec):
    return i + rec['c1'] + rec['c2'] + rec['c3']
  assert(rel.foldl(op, 0) == 30)

  # groupby
  d = rel.groupbyAsRelation(['c1'], ['c2', 'c3'])
  print "aaa"
  print str(d[0,]), str(d[1,]),

  assert(d[0,].size() == 5)
  for c2, c3 in d[0,].getL():
    assert(c2 == 1)
  assert(d[1,].size() == 5)
  for c2, c3 in d[1,].getL():
    assert(c2 == 0)

  # insertL
  a = [(2, 2, 0), (2, 2, 1), (2, 2, 2)]
  rel.insertL(a)
  print rel
  assert(rel.size() == 13)

  # sort
  rel2 = rel.sort(reverse=True)
  print rel2


def joinTwoRelations(keyCols, relCols0, relCols1, reuse=False):
  """
  Join two relations.
  This will execute sort and merge join.

  keyCols :: tuple([str])
      Name of key columns.
      The key columns must be unique in both rel0 and rel1.
  relCols0 :: (Relation, cols)
  relCols1 :: (Relation, cols)
    cols :: [str]
      Target columns.
  reuse :: bool
  return :: Relation
    joined relations.

  """
  rel0, cols0 = relCols0
  rel1, cols1 = relCols1
  assert(isinstance(rel0, Relation))
  assert(isinstance(rel1, Relation))
  assert(util.isList(cols0, str))
  assert(util.isList(cols1, str))
  assert(len(cols0) > 0)
  assert(len(cols1) > 0)
  assert(isinstance(keyCols, tuple))
  assert(len(keyCols) > 0)

  # sorted records generator.
  schema0 = list(keyCols) + cols0
  schema1 = list(keyCols) + cols1
  g0 = iter(rel0.sort(cols=list(keyCols)).project(schema0).getG())
  g1 = iter(rel1.sort(cols=list(keyCols)).project(schema1).getG())

  # merged records generator.
  keySize = len(keyCols)
  def joinedRawRecG():
    isEnd = False
    try:
      rawRec0 = g0.next()
      rawRec1 = g1.next()
    except StopIteration:
      isEnd = True
      for _ in []:
        yield
    while not isEnd:
      try:
        if rawRec0[:keySize] == rawRec1[:keySize]:
          yield rawRec0[:keySize] + rawRec0[keySize:] + rawRec1[keySize:]
          rawRec0 = g0.next()
          rawRec1 = g1.next()
        elif rawRec0[:keySize] < rawRec0[:keySize]:
          rawRec0 = g0.next()
        else:
          rawRec1 = g1.next()
      except StopIteration:
        isEnd = True

  retSchemaEntryL = []
  for colName in list(keyCols) + cols0:
    retSchemaEntryL.append(rel0.schema().getEntry(colName))
  for colName in cols1:
    retSchemaEntryL.append(rel1.schema().getEntry(colName))
  return Relation(Schema(retSchemaEntryL), joinedRawRecG(), reuse=reuse)

def testJoinRel():
  pass


def joinRelations(keyCols, relColsList, reuse=False):
  """
  Join multiple relations with a key.
  This will execute sort and merge join n-1 times
  where n is len(relColsList)

  keyCols :: tuple([str])
    The key must be unique.
  relColsList :: [(rel, cols)]
    rel :: Relation
    cols :: [str]
      target columns.
    Each column name must be unique.
  return :: Relation
    joined relation with the key and target columns.

  """
  assert(isinstance(keyCols, tuple))
  for col in keyCols:
    assert(isinstance(col, str))
  assert(isinstance(relColsList, list))
  assert(len(relColsList) >= 2)
  for rel, cols in relColsList:
    assert(isinstance(rel, Relation))
    assert(isinstance(cols, list))
    for col in cols:
      assert(isinstance(col, str))

  relCols0 = relColsList[0]
  for relCols1 in relColsList[1:]:
    relCols0 = joinTwoRelations(keyCols, relCols0, relCols1, reuse=False)

  if reuse:
    return Relation(relCols0.schema(), relCols0.getG(), reuse=True)
  else:
    return relCols0


def testJoinRelations():

  schema = Schema.parse('#k1::Integer k2::Integer v1::Integer')
  rawRecL = [(x, x, x) for x in range(0, 3)]
  rel0 = Relation(schema, rawRecL, reuse=True)
  print rel0.show()

  schema = Schema.parse('#k1::Integer k2::Integer v2::Integer')
  rawRecL = [(x, y, x + y)
             for x in range(0, 3)
             for y in range(0, 3)]
  rel1 = Relation(schema, rawRecL, reuse=True)
  print rel1.show()

  joined = joinRelations(('k1', 'k2'),
                         [(rel0, ['v1']), (rel1, ['v2'])], reuse=True)
  print joined.show()

  schema = Schema.parse('#k1::Integer k2::Integer v1::Integer v2::Integer')
  rawRecL = [(x, x, x, x + x) for x in range(0, 3)]
  answer = Relation(schema, rawRecL, reuse=True)

  assert(all(map(lambda (x,y): x == y, zip(joined.getL(), answer.getL()))))

def doMain():
  testSchema()
  testColumn()
  testRecord()
  testIterableData()
  testRelation()
  testJoinRelations()

if __name__ == '__main__':
  doMain()
