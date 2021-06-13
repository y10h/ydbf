# encoding: utf-8
# YDbf - Pythonic reader and writer for DBF/XBase files
#
# Copyright (C) 2006-2021 Yury Yurevich and contributors
#
# https://github.com/y10h/ydbf
"""
Pythonic reader and writer for DBF/XBase files

YDbf provides clear API to read and write DBF files
as a stream.

Reading
-------

The entrypoint of YDbf is `open` function:

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

Each record is a dict, which keys are names of fields.

Writing
-------

YDbf opens file for reading by default, but you may set option `mode` to
open for writing:

    dbf = ydbf.open('simple.dbf', ydbf.WRITE, fields)

or open file yourself:

    fh = open('simple.dbf', 'wb')
    dbf = ydbf.open(fh, ydbf.WRITE, fields)

`fields` is a structure description of DBF file, it is a required option for
write mode. The structure is as sequence of field descriptions,
where each fields described by tuple (NAME, TYPE, SIZE, DECIMAL). NAME
is a name of field, TYPE -- DBF type of field ('N' for number, 'C' for char,
'D' for date, 'L' for logical), DECIMAL is a precision (useful for 'N' type only).

YDbf offers the field types as constants:
 - ydbf.CHAR
 - ydbf.DATE
 - ydbf.LOGICAL
 - ydbf.NUMERAL

An example of the fields definition:

    fields = [
        ('ID',      ydbf.NUMERAL,  4, 0),
        ('VALUE',   ydbf.CHAR, 40, 0),
        ('UPDATE',  ydbf.DATE, 8, 0),
        ('VISIBLE', ydbf.LOGICAL, 1, 0),
    ]


YDbf uses unicode for 'C' fields by default, so you may want to define
encoding which be used forthe  DBF file. UTF-8 is not supported, you may
use only 8-bit encodings.

    dbf = ydbf.open('simple.dbf', ydbf.WRITE, fields, encoding='cp1251')
    dbf.write(data)

YDbf gets `data` as an iterator where each item is a dict, which
keys are name of fields. For example,

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

import builtins

# Import types constants from lib so they are available at package level.
from ydbf.lib import CHAR
from ydbf.lib import DATE
from ydbf.lib import LOGICAL
from ydbf.lib import NUMERAL


from ydbf.reader import YDbfReader
from ydbf.writer import YDbfWriter

READ = 'r'
WRITE = 'w'

FILE_MODES = {
    READ: YDbfReader,
    WRITE: YDbfWriter,
}


def open(dbf_file, mode=READ, *args, **kwargs):
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
            [(NAME, TYPE, SIZE, DECIMAL), ...]
        
        `use_unicode`:
            Use unicode for string data, True by default
        
        `encoding`:
            Set encoding of DBF file, most
            useful for writing mode.
    """
    if mode not in FILE_MODES:
        raise ValueError("Wrong mode %s for ydbf.open" % mode)
    dbf_class = FILE_MODES[mode]
    if isinstance(dbf_file, str):
        fh = builtins.open(dbf_file, '{mode}b'.format(mode=mode))
        return dbf_class(fh, *args, **kwargs)
    else:
        return dbf_class(dbf_file, *args, **kwargs)
