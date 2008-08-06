#!/usr/bin/env python
# -*- coding: utf-8 -*-
# YDbf - Yielded dbf reader-writer
# Inspired by code of Raymond Hettinger
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/362715
#
# Copyright (C) 2006-2008 Yury Yurevich and contributors
#
# http://www.pyobject.ru/projects/ydbf/
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation, version 2
# of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
"""
YDbf dumper script
"""
import sys
from optparse import OptionParser
from ydbf import VERSION
from ydbf.reader import YDbfStrictReader


def parse_options(args):
    """
    Parse options
    """
    parser = OptionParser(usage="%prog [options] files", version="%%prog %s" % VERSION)
    parser.add_option('-r', '--rs', 
                           dest='record_separator',
                           type='string',
                           default='\n',
                           help='output record separator [default newline]')
    parser.add_option('-f', '--fs', 
                           dest='field_separator',
                           default=':',
                           help='output field separator [default colon]',
                           ),
    parser.add_option('-F', '--fields',
                           dest='fields',
                           default='',
                           type='string',
                           help='comma separated list of fields to print [default all]',
                           )
    parser.add_option('-u', '--undef', 
                           dest='undef',
                           type='string',
                           default='',
                           help='string to print for NULL values [default emptystring]'
                           ),
    parser.add_option('-t', '--table', 
                           dest='table',
                           action='store_true',
                           default=False,
                           help='output in table format [default false]'),
    parser.add_option('-o', '--output',
                           dest='output',
                           type='string',
                           default='',
                           help='output file'
                           )
    options, args = parser.parse_args(args)
    if not args:
        parser.error('Files is required argument')       
    return options, args

def csv_output_generator(data_iterator, record_separator, field_separator):
    for rec in data_iterator:
        yield field_separator.join(str(f) for f in rec) + record_separator

def table_output_generator(fields_spec, data_iterator):
    # either separators do not acts on table output, this options only for keeping
    # interface similar to csv_output_generator.
    place_holders = []
    header_data = []
    names = []
    # delimiter is similar to field_separator, but used in table
    delimiter = ' | ' 
    newline = '\n' # maybe better use os.linesep?
    for name, type_, length, dec in fields_spec:
        place_holders.append(
            (type_ != 'N' and '%% -%ds' % length) or
            (dec and '%%-%d.%df' % (length, dec)) or
            '%%-%dd' % length
        )
        names.append(name)
        # make data for header
        if type_ == 'D':
            length = 10
        if len(name) > length:
            name = name[:length-1] + '+'
        format = '%% -%ds' % length
        header_data.append(format % name)
    # add newline at and of file
    place_holders.append(newline)
    header_data.append(newline)
    format_string = delimiter.join(place_holders)
    header = delimiter.join(header_data)
    # send header to output
    yield header
    # send line between header and data
    yield '-'*(len(header)-2) + newline
    for rec in data_iterator:
        # send single record
        yield format_string % tuple(rec)

def _filter_fields(data_iterator, dbf_fields, fields_to_show):
    for rec in data_iterator:
        filtered_rec = tuple(
            value 
            for (name, type_, length, dec), value in zip(dbf_fields, rec) 
            if name.lower() in fields_to_show
        )
        yield filtered_rec

def dbf_data(fh, fields=None):
    # we don't want to use YDbfReader, because
    # we don't need either as_dict, no use_unicode
    reader = YDbfStrictReader(fh)
    if fields:
        fields_to_show = [f.lower() for f in fields]
        fields_spec = [f for f in reader.fields if f[0].lower() in fields_to_show]
        generator = _filter_fields(reader(), reader.fields, fields_to_show)        
    else:
        fields_spec = reader.fields
        generator = reader()
    return fields_spec, generator

def write_output(output_fh, data_iterator, flush_on_each_record=True):
    for rec in data_iterator:
        output_fh.write(rec)
        if flush_on_each_record:
            output_fh.flush()

def dump(args):
    options, args = parse_options(args)
    for filename in args:
        fh = open(filename, 'rb')
        fields_spec, data_iterator = dbf_data(fh)
        if options.table:
            output_generator = table_output_generator(fields_spec, data_iterator)
        else:
            output_generator = csv_output_generator(data_iterator, options.record_separator, options.field_separator)
        if options.output:
            ofh = open(options.output, 'w')
        else:
            ofh = sys.stdout
        write_output(ofh, output_generator)        
        
if __name__ == '__main__':
    dump(sys.argv[1:])

