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
Common lib for both reader and writer
"""

import datetime

# Reference data

ENCODINGS = {
    # got from dbf description [dbfspec]
    # id      name      description
    0x00:    ('ascii', 'ASCII'), # internal use
    0x01:    ('cp437', 'DOS USA'),
    0x02:    ('cp850', 'DOS Multilingual'),
    0x03:    ('cp1252', 'Windows ANSI'),
    0x04:    ('mac_roman', 'Standard Macintosh'), # NOT SHURE
    0x64:    ('cp852', 'EE MS-DOS'),
    0x65:    ('cp866', 'Russian MS-DOS'),
    0x66:    ('cp865', 'Nordic MS-DOS'),
    0x67:    ('cp861', 'Icelandic MS-DOS'),
    0x6A:    ('cp737', 'Greek MS-DOS (437G)'),
    0x6B:    ('cp857', 'Turkish MS-DOS'),
    0x96:    ('mac_cyrillic', 'Russian Macintosh'),
    0x97:    ('mac_latin2', 'Eastern Europe Macintosh'), # NOT SHURE
    0x98:    ('mac_greek', 'Greek Macinstosh'),
    0xC8:    ('cp1250', 'Windows EE'),
    0xC9:    ('cp1251', 'Russian Windows'),
    0xCA:    ('cp1254', 'Turkish Windows'),
    0xCB:    ('cp1253', 'Greek Windows'),
    ## these encodings doesn't supported by python
    # 0x68:    ('cp895', 'Kamenicky (Czech) MS-DOS'),
    # 0x69:    ('cp790', 'Mazovia (Polish) MS-DOS'),
}

REVERSE_ENCODINGS = dict([(value[0], (code, value[1]))
                          for code, value in ENCODINGS.items()])

SIGNATURES = {
    0x02: 'FoxBase',
    0x03: 'dBASE III',
    0x04: 'dBASE IV',
    0x05: 'dBASE V',
    0x30: 'Visual FoxPro',
    0x31: 'Visual FoxPro with AutoIncrement field',
    0x43: 'dBASE IV with SQL table and memo file',
    0x7B: 'dBASE IV with memo file',
    0x83: 'dBASE III with memo file',
    0x8B: 'dBASE IV with memo file',
    0x8E: 'dBASE IV with SQL table',
    0xB3: '.dbv and .dbt memo (Flagship)',
    0xCB: 'dBASE IV with SQL table and memo file',
    0xE5: 'Clipper SIX driver with SMT memo field',
    0xF5: 'FoxPro with memo field',
    0xFB: 'FoxPro',
}

SUPPORTED_SIGNATURES = (0x03, 0x04, 0x05)

# <   -- little endian
# B   -- version number (signature)
# 3B  -- last update (YY, MM, DD)
# L   -- number of records
# H   -- length of header
# H   -- length of each record
# 17x -- pad (2B -- reserved,
#              B -- incomplete transaction,
#              B -- encryption flag,
#             4B -- free record thread (reserved for LAN)
#             8B -- reserved for multiuser dBASE
#              B -- MDX flag)
# B   -- language driver
# 2x  -- pad (2B -- reserved)
HEADER_FORMAT = '<B3BLHH17xB2x'

# <   -- little endian
# 11s -- field name in ASCII (terminated by 0x00)
# c   -- field type (ASCII)
# 4x  -- field data address ( 2B -- address in memory (for dBASE)
#                          OR 4B -- offset of field from
#                                   beginning of record (for FoxPro)
# B   -- field length
# B   -- decimal count
# 14x -- pad (2B -- reserved for multi-user dBASE,
#              B -- work area id ()
#             2B -- reserved for multi-user dBASE,
#              B -- flag for SET FIELDS
#             7B -- reserved
#              B -- index field flag)
# B   -- language driver
# 2x  -- pad (2B -- reserved)
FIELD_DESCRIPTION_FORMAT = '<11sc4xBB14x'

# Common functions

def dbf2date(dbf_str):
    """
    Converts date from dbf-date to datetime.date
    
    Args:
        `dbf_str`:
            string in format YYYYMMDD
    """
    if dbf_str is None or not dbf_str.isdigit() or len(dbf_str) != 8:
        result = None
    else:
        result = datetime.date(int(dbf_str[:4]),
                               int(dbf_str[4:6]),
                               int(dbf_str[6:8]))
    return result

def date2dbf(dt):
    """
    Converts date from datetime.date to dbf-date (string in format YYYYMMDD)
    
    Args:
        `dt`:
            datetime.date instance
    """
    if not isinstance(dt, datetime.date):
        raise TypeError("Espects datetime.date instead of %s" % type(dt))
    return "%04d%02d%02d" % (dt.year, dt.month, dt.day)
    
def dbf2str(dbf_str):
    """
    Converts date from dbf-date to string (DD.MM.YYYY)
    
    Args:
        `dbf_str`:
            dbf-date (string in format YYYYMMDD)
    """
    if dbf_str is None or not dbf_str.isdigit() or len(dbf_str) != 8:
        result = None
    else:
        result = ".".join( reversed( (dbf_str[:4],
                                      dbf_str[4:6],
                                      dbf_str[6:8]) ) )
    return result

def str2dbf(dt_str):
    """
    Converts from string to dbf-date (string in format YYYYMMDD)
    
    Args:
        `dt_str`:
            string in format DD.MM.YYYY
    """
    if not isinstance(dt_str, basestring):
        raise TypeError("Espects string or unicode instead of %s"
                         % type(dt_str))
    str_l = len(dt_str)
    if str_l != 10:
        raise ValueError('Datestring must be 10 symbols (DD.MM.YYYY) '
                         'length instead of %d' % str_l)
    d, m, y = dt_str.split('.')
    return ''.join((y, m, d))

# References:
# [dbfspec]: http://www.clicketyclick.dk/databases/xbase/format/index.html

