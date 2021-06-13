YDbf
====

YDbf - reading and writing DBF/XBase files in a pythonic way.
The library written in pure Python and have no external
dependencies.

YDbf is compatible with Python 3.5+. The last 2.7-compatible
version of YDbf is 0.4.1.

What YDbf is good for:

 - export data to a DBF file
 - import data from a DBF file
 - read data from a DBF file as a stream

Where YDbf is not a good fit:

 - random access to records in a DBF file
 - memo fields

Read DBF
--------

The entrypoint of YDbf is `open` function:

    dbf = ydbf.open('simple.dbf')

You can use file name, or already opened in binary mode file:

    fh = open('simple.dbf', 'rb')
    dbf = ydbf.open(fh)
    
    for record in dbf:
        ...

You may also use `with` statement:

    with ydbf.open('simple.dbf') as dbf:
        for record in dbf:
            ...

Each record is a dict, which keys are names of fields.

Write DBF
---------

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

