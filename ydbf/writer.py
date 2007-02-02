# -*- coding: utf-8 -*-
# YDbf - Yielded dbf reader-writer
# Inspired by code of Raymond Hettinger
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/362715
#
# Copyright (C) 2006 Yury Yurevich, Alexandr Zamaraev
# Copyright (C) 2007 Yury Yurevich
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
DBF writer
"""
__revision__ = "$Id$"
__url__ = "$URL$"

import struct
import itertools
import datetime

from ydbf.lib import date2dbf

class YDbfWriter(object):
    '''
    Class for writing DBF
    '''
    def __init__(self, fh, fields):
        '''
        Make DBF-creator
        @param fh: filehandler, should be opened for binary writing
        @param fields: fields structure in format
            [(NAME, TYP, SIZE, DEC), ...]
        @type NAME: string
        @type TYP: string in ("N", "D", "C", "L")
        @type SIZE: integer
        @type DEC: integer
        '''
        self.now = datetime.date.today()
        self.numrec = 0
        self.numfields = len(fields)
        self.lenheader = self.numfields * 32 + 33
        self.recsize = sum([field[2] for field in fields]) + 1
        self.date2dbf = date2dbf
        self.ver = 3
        self.fh = fh
        self.fields = fields
            
    def writeHeader(self):
        '''
        Write DBF-header
        '''
        pos = self.fh.tell()
        self.fh.seek(0)
        year, month, day = self.now.year-1900, self.now.month, self.now.day
        self.hdr = struct.pack('<BBBBLHH20x', self.ver, year, month,
                               day, self.numrec, self.lenheader,
                               self.recsize)
        self.fh.write(self.hdr)
        for name, typ, size, deci in self.fields:
            name = name.ljust(11).replace(' ', '\x00')   # compability with py23, for py24 it looks like name.ljust(11, '\x00')
            fld = struct.pack('<11sc4xBB14x', name, typ, size, deci)
            self.fh.write(fld)
        # terminator
        self.fh.write('\r')
        if pos > 0:
            self.fh.seek(pos)
    
    def __call__(self, records):
        '''
        Run DBF-creator
        @param records: iterator over records 
            (each record is tuple or sequense of values)
        '''
        self.writeHeader()
        i = 0
        for record in records:
            i += 1
            self.fh.write(' ')                        # deletion flag
            for (name, typ, size, deci), value in itertools.izip(self.fields, record):
                if typ ==   'N':
                    if value is None:
                        value = 0
                    if deci:
                        pattern = "%%.%df" % deci # ->%.2f 
                        value = pattern % value
                    else:
                        value = int(value)
                    value = str(value).rjust(size)
                elif typ == 'D':
                    if value is None:
                        value = '00000000'
                    else:
                        value = self.date2dbf(value)
                elif typ == 'L':
                    value = str(value)[0].upper()
                else:
                    if value is None:
                        value = ''
                    value = str(value)[:size].ljust(size)
                if len(value) != size:
                    raise ValueError("value '%s' of %s not aligned to %d" % (str(value), name, size))
                self.fh.write(value)
        self.numrec = i
        self.writeHeader()
        # End of file
        self.fh.write('\x1A')
