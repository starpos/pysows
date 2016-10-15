#!/usr/bin/python2
# -*- coding: utf-8 -*-

import sys
import argparse


def parse_tsv_line(line, delimiters, separator, ignore_error):
    ret = {}
    for kv in line.strip().split(delimiters):
        i = kv.find(separator)
        if i < 0:
            if ignore_error:
                continue
            else:
                raise RuntimeError('separator not found: ', line, separator)
        k = kv[:i]
        v = kv[i + 1:]
        ret[k] = v
    return ret


class END(Exception):
    pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', nargs='?', metavar='INPUT_FILE', help='stdin will be used if omitted.')
    parser.add_argument('-c', '--columns', default=None, help='column names concatinated by comma to select.')
    parser.add_argument('-d', '--delimiters', default=None, help='delimiters.')
    parser.add_argument('-s', '--separator', default=':', help='key-value separator (default colon).')
    parser.add_argument('-i', '--ignore-error', action='store_true', help='ignore invalid lines.')
    ns = parser.parse_args()

    columns = []
    if ns.columns is not None:
        columns = ns.columns.split(',')
    if ns.input_file is not None:
        in0 = open(ns.input_file, "r")
    else:
        in0 = sys.stdin

    line = in0.readline()
    while line:
        try:
            tsv = parse_tsv_line(line, ns.delimiters, ns.separator, ns.ignore_error)
            if tsv is None:
                continue
            tup = []
            for col in columns:
                if col not in tsv:
                    if ns.ignore_error:
                        raise END
                    else:
                        raise RuntimeError('invalid line:', line)
                tup.append(tsv[col])
            print '\t'.join(tup)
        except END:
            pass
        line = in0.readline()

if __name__ == '__main__':
    main()

