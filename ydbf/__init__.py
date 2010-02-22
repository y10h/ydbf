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
Pythonic reader and writer for DBF/XBase files

YDbf provides clear API to read and write DBF files.

Reading
-------

The entrypoint of YDbf is an `open` function:

    dbf = ydbf.open('simple.dbf')

You can use file name, or already opened (in binary mode) file:

    fh = open('simple.dbf', 'rb')
    dbf = ydbf.open(fh)
    
    for record in dbf:
        ...

If you have Python 2.5+, you may want to use `with` statement:

    with ydbf.open('simple.dbf') as dbf:
        for record in dbf:
            ...

Each record is represented as dict, where keys are names of fields.

Writing
-------

By default, YDbf opens file for reading, but you may set option `mode` to
open for writing:

    dbf = ydbf.open('simple.dbf', 'w', fields)

or open file yourself:

    fh = open('simple.dbf', 'wb')
    dbf = ydbf.open(fh, 'w', fields)

`fields` is a structure description of DBF file, it is required option for
writing mode. Structure defined as sequence of field descriptions,
where each fields described by tuple (NAME, TYP, SIZE, DEC). Where NAME
is a name of field, TYP -- DBF type of field ('N' for number, 'C' for char,
'D' for date, 'L' for logical), DEC is a precision (useful for 'N' type only).
For example, like this:

    fields = [
        ('ID',      'N',  4, 0),
        ('VALUE',   'C', 40, 0),
        ('UPDATE',  'D', 8, 0),
        ('VISIBLE', 'L', 1, 0),
    ]

YDbf uses unicode for 'C' fields by default, so you may want to define
encoding which be used for DBF file. Sadly, UTF-8 is not available, only
old scrappy 8-bits.

    dbf = ydbf.open('simple.dbf', 'w', fields, encoding='cp1251')
    dbf.write(data)

YDbf gets `data` as iterator where each item is a dict, where
keys are name of fields. For early defined example it may looks like

    data = [
        {'ID': 1, 'VALUE': u'ydbf', 'VISIBLE': True,
         'UPDATE': datetime.date(2009, 7, 14)},
        {'ID': 2, 'VALUE': u'ydbf-dev', 'VISIBLE': False,
         'UPDATE': datetime.date(2009, 5, 15)},
        {'ID': 3, 'VALUE': u'pytils', 'VISIBLE': True,
         'UPDATE': datetime.date(2009, 5, 11)},
    ]
"""
try:
    import pkg_resources
    try:
        VERSION = pkg_resources.get_distribution('YDbf').version
    except pkg_resources.DistributionNotFound:
        VERSION = 'unknown'
except ImportError:
    VERSION = 'N/A'
    
from ydbf.reader import YDbfReader
from ydbf.writer import YDbfWriter

FILE_MODES = {
    'r': YDbfReader,
    'w': YDbfWriter,
}

def open(dbf_file, mode='r', *args, **kwargs):
    """
    Open DBF for reading or writing
    
    Args:
        `dbf_file`:
            file name or file-like object
        
        `mode`:
            'r' for reading, 'w' for writing
        
        `fields`:
            fields structure of DBF file, most
            useful for writing mode, defined as
            [(NAME, TYP, SIZE, DEC), ...]
        
        `use_unicode`:
            Use unicode for string data, True by default
        
        `encoding`:
            Set encoding of DBF file, most
            useful for writing mode.
    """
    if mode not in FILE_MODES:
        raise ValueError("Wrong mode %s for ydbf.open" % mode)
    if isinstance(dbf_file, basestring):
        dbf_file = file(dbf_file, '%sb' % mode)
    dbf_class = FILE_MODES[mode]
    return dbf_class(dbf_file, *args, **kwargs)
