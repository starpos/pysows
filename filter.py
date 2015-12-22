#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Filter a list of record as an input strem.

"""

import sys
import re
import argparse
import pysows

def parseOpts(args):
    """
    args :: [str]
        argument string list
    return :: argparse.Namespace

    """
    parser = argparse.ArgumentParser(
        description="Filter a list of record as an input stream.")
    pysows.setVersion(parser)
    parser.add_argument("-g", "--groups", dest="group_indexes",
                        metavar='COLUMNS', default='0',
                        help=pysows.GROUPS_HELP_MESSAGE)
    parser.add_argument("-p", "--predicate", dest="predicate",
                        metavar='PREDICATE',
                        default='lambda *xs:True',
                        help="Predicate as python code." + \
                            " This must return bool value.")
    parser.add_argument("-r", "--regex", dest="regex_list",
                        nargs='+', metavar='REGEX_PATTERN', default=None,
                        help="Regular expression." + \
                            " When both -p and -r options are specified, -r is used.")
    parser.add_argument("-l", "--load", dest="load_file",
                        metavar='FILE', default=None,
                        help="Load python code for -p.")
    parser.add_argument("-s", "--separator", dest="separator",
                        metavar='SEP', default=None,
                        help="Record separator. (default: spaces)")
    parser.add_argument("-v", "--invert", action="store_true", dest="invert",
                        default=False,
                        help="Invert match like grep -v.")
    return parser.parse_args(args)

def generateFilterByPredicate(predicateStr, convIdxL, globalNamespace, localNamespace):
    """
    predicateStr :: str
        Predicate string.
        After evaled, predicate type must be (*xs -> bool).
    convIdxL :: [(str -> ANY), int]
        Column index list with type converter.
    globalNamespace :: dict
        Global name space.
    localNamespace :: dict
        local name space.
    return :: tuple(str) -> bool
        Filter function.

    """
    predicate = eval(predicateStr, globalNamespace, localNamespace)
    project1 = pysows.generateProjectConv(convIdxL)

    def filterByPredicate(rec):
        """
        rec :: tuple(str)
        return :: bool

        """
        return predicate(*project1(rec))
    return filterByPredicate


def generateFilterByRegex(regexStrL, convIdxL):
    """
    regexStrL :: [str]
        Regular expression string list.
    convIdxL :: [(str -> ANY), int]
        Column index list with converter.
        converters will be ignored.
        Specify 1 for first element.
        0 means all columns.
    separator :: str
        Separator string of columns in a line.
    return :: str -> bool
        Filter function.

    """
    regexL = map(re.compile, regexStrL)
    idxL = map(lambda (_, idx): idx, convIdxL)
    project1 = pysows.generateProject(idxL)

    def filterByRegex(rec):
        """
        rec :: tuple(str)
        return :: bool

        """
        rec1 = project1(rec)
        if len(regexL) < len(rec1):
            raise IOError("key length %d > number of regex %d." % (len(rec1), len(regexL)))

        predicate = lambda (regex, x): regex.match(x) is not None
        return all(map(predicate, zip(regexL, rec1)))

    return filterByRegex

def doMain():
    args = parseOpts(sys.argv[1:])

    convIdxL = pysows.getTypedColumnIndexList(args.group_indexes)

    g = globals()
    l = locals()
    pysows.loadPythonCodeFile(args.load_file, g, l)

    if args.regex_list is None:
        filterBy = generateFilterByPredicate(args.predicate, convIdxL, g, l)
    else:
        filterBy = generateFilterByRegex(args.regex_list, convIdxL)
    if args.invert:
        notIfInvert = lambda x: not x
    else:
        notIfInvert = lambda x: x

    for rec in pysows.recordReader(sys.stdin, args.separator):
        if notIfInvert(filterBy(rec)):
            pysows.printList(rec)
            print

if __name__ == "__main__":
    try:
        doMain()
    except Exception, e:
        pysows.exitWithError(e)
