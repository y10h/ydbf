#!/usr/bin/env python
# -*- coding: utf-8 -*-
# YDbf - Pythonic reader and writer for DBF/XBase files
# Inspired by code of Raymond Hettinger
# http://code.activestate.com/recipes/362715
#
# Copyright (C) 2006-2010 Yury Yurevich and contributors
#
# http://pyobject.ru/projects/ydbf/
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
from ydbf import lib, VERSION
from ydbf.reader import YDbfStrictReader

def _unescape_separator(option, opt_str, value, parser):
    """
    Unescape special symbols (like newline)
    
    When --rs '\n' passed to ydbf.dump, OptionParser saves it as
    '\\n', i.e. escapes it. We want to unescape special symbols
    like '\n', '\r', '\t' both in record and field separators
    """
    if value is not None:
        replacements = (('\\n', '\n'), ('\\r', '\r'), ('\\t', '\t'))
        for what, replace_by in replacements:
            value = value.replace(what, replace_by)
    setattr(parser.values, option.dest, value)

def _split_fields(option, opt_str, value, parser):
    """
    Split value of option -F (fields) to list of strings
    """
    if value:
        value = tuple(f.upper().strip() for f in value.split(','))
    setattr(parser.values, option.dest, value)

def show_info(files):
    """
    Show info about files
    """
    for f in files:
        reader = YDbfStrictReader(open(f, 'rb'))
        header_info = {
            'filename': f,
            'signature': hex(reader.sig),
            'version': lib.SIGNATURES.get(reader.sig, 'N/A'),
            'lang_code': hex(reader.raw_lang),
            'encoding': lib.ENCODINGS.get(reader.raw_lang, ('n/a', 'N/A'))[0],
            'language': lib.ENCODINGS.get(reader.raw_lang, ('n/a', 'N/A'))[1],
            'records_number': str(reader.numrec),
            'header_length': str(reader.lenheader),
            'record_length': str(reader.recsize),
            'last_change': str(reader.dt),
            'fields_number': str(reader.numfields),
        }
        print """\
Filename:       %(filename)s
Version:        %(signature)s (%(version)s)
Encoding:       %(lang_code)s (%(encoding)s, %(language)s)
Num of records: %(records_number)s
Header length:  %(header_length)s
Record length:  %(record_length)s
Last change:    %(last_change)s
Num of fields:  %(fields_number)s
===========================================
Num   Name                Type Len  Decimal
-------------------------------------------""" % header_info

        for i, (name, type_, length, dec) in enumerate(reader.fields):
            print "% 3d.  %s  %s  %s  %d" % \
                (i+1, name.ljust(20), type_, str(length).rjust(3), dec)

def parse_options(args):
    """
    Parse options
    """
    parser = OptionParser(usage="%prog [options] files", version="%%prog %s"
                                                                 % VERSION)
    parser.add_option('-r', '--rs',
                           dest='record_separator',
                           action='callback',
                           callback=_unescape_separator,
                           default='\n',
                           type='string',
                           help='output record separator [default newline]')
    parser.add_option('-f', '--fs',
                           dest='field_separator',
                           action='callback',
                           callback=_unescape_separator,
                           default=':',
                           type='string',
                           help='output field separator [default colon]',
                           ),
    parser.add_option('-F', '--fields',
                           dest='fields',
                           action='callback',
                           callback=_split_fields,
                           default='',
                           type='string',
                           help='comma separated list of fields to print ' \
                                '[default all]',
                           )
    parser.add_option('-u', '--undef',
                           dest='undef',
                           type='string',
                           default='',
                           help='string to print for NULL values ' \
                                '[default emptystring]'
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
    parser.add_option('-i', '--info',
                           dest='info',
                           action='store_true',
                           default=False,
                           help='show info about file and exit'),
    options, args = parser.parse_args(args)
    if not args:
        parser.error('Files is required argument')
    if options.info:
        show_info(args)
        sys.exit(0)
    return options, args

def csv_output_generator(data_iterator, record_separator, field_separator):
    """
    Make CSV-like output with specified record and field separators
    """
    for rec in data_iterator:
        yield field_separator.join(str(f) for f in rec) + record_separator

def table_output_generator(fields_spec, data_iterator):
    """
    Make table-look output
    """
    # either separators do not acts on table output, this options only
    # for keeping interface similar to csv_output_generator.
    place_holders = []
    header_data = []
    names = []
    # delimiter is similar to field_separator, but used in table
    delimiter = ' | '
    newline = '\n' # maybe better use os.linesep?
    # for some types maximal length is fixed, use it
    fixed_length_types = {
        'D': 10,
        'L': 5,
    }
    for name, type_, length, dec in fields_spec:
        if type_ in fixed_length_types:
            length = fixed_length_types[type_]
        if type_ != 'N':
            holder = '%% -%ds' % length
        else:
            if dec:
                holder = '%%-%d.%df' % (length, dec)
            else:
                holder = '%%-%dd' % length
        place_holders.append(holder)
        names.append(name)
        # make data for header
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

def _escape_data(data_iterator, symbol_escape_to):
    """
    Escapes field separator in data
    """
    for rec in data_iterator:
        yield tuple(str(x).replace(symbol_escape_to,
                                   '\\%s' % symbol_escape_to) for x in rec)

def replace_null(data_iterator, undef):
    """
    Replace NULL (None) values by undef string
    """
    def provide_undef(v):
        if v is None:
            return undef
        else:
            return v
    for rec in data_iterator:
        yield tuple(provide_undef(x) for x in rec)

def _flatten_data(data_iterator, fields):
    """
    Flatten each rec in data iterator from dict to tuple
    """
    for rec in data_iterator:
        yield tuple(rec[name] for name in fields)

def dbf_data(fh, fields=None):
    """
    Return a fields spec and data generator
    """
    reader = YDbfStrictReader(fh, use_unicode=False)
    if fields:
        fields_spec = [f for f in reader.fields if f[0] in fields]
        if len(fields_spec) != len(fields):
            # got wrong name in fields
            difference = tuple(set(fields) - set(f[0] for f in fields_spec))
            if difference:
                raise ValueError("Wrong fields: %s" % ', '.join(difference))
            else:
                raise ValueError("Wrong fields")
    else:
        # all fields
        fields_spec = reader.fields
        fields = [f[0] for f in reader.fields]
    generator = _flatten_data(reader.records(), fields)
    return fields_spec, generator

def write_output(output_fh, data_iterator, flush_on_each_record=True):
    """
    Write data from data_iterator to output_fh
    """
    for rec in data_iterator:
        output_fh.write(rec)
        if flush_on_each_record:
            output_fh.flush()

def dump(args):
    """
    Dump DBF file
    """
    options, args = parse_options(args)
    if options.output:
        ofh = open(options.output, 'w')
    else:
        ofh = sys.stdout
    for filename in args:
        fh = open(filename, 'rb')
        fields_spec, data_iterator = dbf_data(fh, options.fields)
        data_iterator = replace_null(data_iterator, options.undef)
        if options.table:
            output_generator = table_output_generator(fields_spec,
                                                      data_iterator)
        else:
            data_iterator = _escape_data(data_iterator,
                                         options.field_separator)
            output_generator = \
                csv_output_generator(
                        data_iterator,
                        options.record_separator,
                        options.field_separator
                    )
        write_output(ofh, output_generator)

def main():
    dump(sys.argv[1:])

if __name__ == '__main__':
    main()

