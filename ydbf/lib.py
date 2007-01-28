# -*- coding: utf-8 -*-
# YDbf - Yielded dbf reader-writer
# Copyright (C) 2006-2007 Yury Yurevich
#
# http://gorod-omsk.ru/blog/pythy/projects/ydbf/
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
__revision__ = "$Id$"
__url__ = "$URL$"

import datetime

ENCODINGS = {
    # got from dbf description [dbfspec], [rudbfspec]
    # id      name      description
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

REVERSE_ENCODINGS = dict([(value[0], (code, value[1])) for code, value in ENCODINGS.items()])

SIGNATURES = {
    0x02: 'FoxBase',
    0x03: 'dBASE III',
    0x04: 'dBASE IV',
    0x05: 'dBASE IV',
    0x30: 'Visual FoxPro',
    0x31: 'Visual FoxPro with AutoIncrement field',
    0x43: 'dBASE IV with SQL table and memo file',
    0x7B: 'dBASE IV with memo file',
    0x83: 'dBASE III+ with memo file',
    0x8B: 'dBASE IV with memo file',
    0x8E: 'dBASE IV with SQL table',
    0xB3: '.dbv and .dbt memo (Flagship)',
    0xCB: 'dBASE IV with SQL table and memo file',
    0xE5: 'Clipper SIX driver with SMT memo field',
    0xF5: 'FoxPro with memo field',
    0xFB: 'FoxBase',
}

SUPPORTED_SIGNATURES = (0x03, 0x04, 0x05)

def dbf2date(dbf_str):
    '''
    Convert from dbf-string date to datetime.date
    @param dbf_str: string in format YYYYMMDD
    @return: datetime.date instance
    '''
    if dbf_str is None or not dbf_str.isdigit():
        y, m, d = 1900, 1, 1
        return datetime.date(y, m, d)
        #raise ValueError("'%s' consist of non-int value" % dbf_str)
    if len(dbf_str) == 8:
        y, m, d = int(dbf_str[:4]), int(dbf_str[4:6]), int(dbf_str[6:8])
    else:
        y, m, d = 2200, 1, 1
    return datetime.date(y,m,d)    

def date2dbf(dt):
    '''
    Convert from datetime.date to dbf-string date
    @param dt: datetime.date instance
    @return: string in format YYYYMMDD
    '''
    if not isinstance(dt, datetime.date):
        raise TypeError("Espects datetime.date instead of %s" % type(dt))
    return dt.strftime("%Y%m%d")
    
def dbf2str(dbf_str):
    '''
    Convert from dbf-string to string date (DD.MM.YYYY)
    @param dbf_str: string in format YYYYMMDD
    @return: string in format DD.MM.YYYY
    '''
    
    """'YYYYMMDD' -> 'DD.MM.YYYY'"""
    if dbf_str is None or not dbf_str.isdigit():
        y, m, d = '1900', '01', '01'
        return '.'.join((d,m,y))
        #raise ValueError("'%s' consist of non-int value" % dbf_str)
    if len(dbf_str) == 8:
        y, m, d = dbf_str[:4], dbf_str[4:6], dbf_str[6:8]
    else:
        y, m, d = '2200', '01', '01'
    return '.'.join((d,m,y))

def str2dbf(dt_str):
    '''
    Convert from string date to dbf-string date
    @param dt_str: string in format DD.MM.YYYY
    @return: dbf-string date (string in format YYYYMMDD)
    '''
    if not isinstance(dt_str, basestring):
        raise TypeError("Espects string or unicode instead of %s" % type(dt_str))
    str_l = len(dt_str)
    if str_l != 10:
        raise ValueError('Datestring must be 10 symbols (DD.MM.YYYY) length instead of %d' % str_l)    
    d, m, y = dt_str.split('.')
    return ''.join((y,m,d))

# References:
# [dbfspec]: http://www.clicketyclick.dk/~clicketyclick_dk/databases/xbase/format/index.html
# [rudbfspec]: http://inform.p-stone.ru/libr/db/teoretic/data/public1/