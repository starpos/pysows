#!/usr/bin/env python

__all__ = ['CsvLike']

from relation import Schema, Relation

class CsvLike(Relation):
  """
  CSV-like data.

  1st line must be header starting by '#'
  where column names are listed separated by the separator.
  
  """
  def __init__(self, lineGenerator, sep=None, schema=None):
    """
    lineGenerator :: generator(str)
      CSV-like data.
    sep :: str
       separator for str.split().
    schema :: Schema
       Use the schema.
       Assume there is no header.

    """
    if schema is None:
      schemaLine = lineGenerator.next().rstrip()
      schema = Schema.parse(schemaLine, sep=sep)

    def getRawRecGenerator():
      for line in lineGenerator:
        yield schema.parseValues(line.rstrip().split(sep))
    Relation.__init__(self, schema, getRawRecGenerator(), reuse=True)

def testCsvLike():
  def lg():
    yield '#c1 c2::String c3::Integer c4::Decimal c5::Float'
    yield '1 2 3 4 5'
    yield '2 3 4 5 6'
  rel = CsvLike(lg())
  print rel.schema()
  for raw in rel.getL():
    print raw
  print rel
  
if __name__ == '__main__':
  testCsvLike()
