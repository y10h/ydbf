# -*- coding: utf-8 -*-
# YDbf - Yielded dbf reader-writer
# Inspired by code of Raymond Hettinger
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/362715
#
# Copyright (C) 2006 Yury Yurevich, Alexandr Zamaraev
# Copyright (C) 2007-2008 Yury Yurevich
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
DBF writer
"""
__all__ = ["YDbfBasicWriter", "YDbfWriter"]

import struct
import itertools
import datetime

from ydbf import lib

class YDbfBasicWriter(object):
    """
    Class for writing DBF.
    
        @param fh: filehandler, should be opened for binary writing
        @param fields: fields structure in format
            [(NAME, TYP, SIZE, DEC), ...]
        @type NAME: string
        @type TYP: string in ("N", "D", "C", "L")
        @type SIZE: integer
        @type DEC: integer
    """
    def __init__(self, fh, fields, as_dict=False):
        """
        Constructor
        """
        self.now = datetime.date.today()
        self.numrec = 0
        self.numfields = len(fields)
        self.lenheader = self.numfields * 32 + 33
        self.recsize = sum([field[2] for field in fields]) + 1
        self.date2dbf = lib.date2dbf
        self.sig = 0x03  # signature, DBF 3
        self.lang = 0x0 # w/o lang code
        self.fh = fh
        self.fields = fields
            
    def writeHeader(self):
        """
        Write DBF-header
        """
        pos = self.fh.tell()
        self.fh.seek(0)
        year, month, day = self.now.year-1900, self.now.month, self.now.day
        self.hdr = struct.pack(lib.HEADER_FORMAT, self.sig, year, month,
                               day, self.numrec, self.lenheader,
                               self.recsize, self.lang)
        self.fh.write(self.hdr)
        for name, typ, size, deci in self.fields:
            name = name.ljust(11).replace(' ', '\x00')   # compability with py23, for py24 it looks like name.ljust(11, '\x00')
            fld = struct.pack(lib.FIELD_DESCRIPTION_FORMAT, name, typ, size, deci)
            self.fh.write(fld)
        # terminator
        self.fh.write('\r')
        if pos > 0:
            self.fh.seek(pos)
    
    def __call__(self, records):
        """
        Run DBF-creator
        @param records: iterator over records 
            (each record is tuple or sequense of values)
        """
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
                        value = ' '*8
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
        self.fh.flush()

class DictDeconverter(object):
    """
    Deconvert from dict to list
    
    Must be the last deconverter in chain
    
    @param fields_struct: structure of DBF file
    """
    
    def __init__(self, fields_struct):
        """
        Create dict deconverter
        """
        self.fields = fields_struct
    
    def __call__(self, records_iterator):
        """
        Deconvert from dict each record
        
        @param records_iterator: iterator over DBF records
        """
        for record in records_iterator:
            yield [record[f_name] for f_name, f_type, f_size, f_decimal in self.fields]

class UnicodeDeconverter(object):
    """
    Deconverts all unicode-strings to byte-strings,
    using lang code in DBF file, or implicitly specified encoding
    """
    def __init__(self, encoding='ascii'):
        """
        Create unicode deconverter for DBF writer
        
        @param encoding: encoding (default ascii, 0x0 lang code in DBF) for strings 
        @type encoding: C{str}
        """
        self.encoding = encoding

    def __call__(self, records_iterator):
        """
        Deconvert strings from unicode
        """
        provide_str = lambda x, enc: (isinstance(x, unicode) and x.encode(encoding)) or x
        encoding = self.encoding
        for record in records_iterator:
            yield [provide_str(x, encoding) for x in record]

class YDbfWriter(object):
    """
    Most common DBF writer
        @param fh: filehandler (should be opened for binary reads)
        
        @param use_unicode: use unicode instead of strings, default False
        @type use_unicode: C{boolean}
        
        @param encoding: encoding for DBF file,
            default 'ascii' (or 0x0 war lang code),
        @type encoding: C{str}
        
        @param raw_lang: raw lang code in DBF file
            by default it taken from encoding, but
            using this option you can override it
        @type C{int}
        
        @param as_dict: represent each record as dict instead of list,
            defult False
        @type as_dict: C{boolean}
        
    """

    def __init__(self, fh, fields, **kwargs):
        """
        Create DBF writer
        """
        use_unicode = kwargs.get('use_unicode', False)
        encoding = kwargs.get('encoding', 'ascii')
        raw_lang = kwargs.get('raw_lang')
        if raw_lang is None:
            raw_lang = lib.REVERSE_ENCODINGS.get(encoding, (0x0, 'ASCII'))[0]
        as_dict = kwargs.get('as_dict', False)
        
        self.writer = YDbfBasicWriter(fh, fields)
        self.writer.lang = raw_lang
        
        null_deconverter = lambda xiter: (x for x in xiter)
        
        if use_unicode:
            self.unicode_deconverter = UnicodeDeconverter(encoding)
        else:
            self.unicode_deconverter = null_deconverter
        
        if as_dict:
            self.dict_deconverter = DictDeconverter(fields)
        else:
            self.dict_deconverter = null_deconverter
    
    def __call__(self, records, *args, **kwargs):
        """
        Write records to DBF
        """
        self.writer(self.unicode_deconverter(self.dict_deconverter(records, *args, **kwargs)))
    
    def __getattr__(self, attr):
        if attr in self.__dict__ or attr in ('writer', 'unicode_deconverter', 'dict_deconverter',):
            return self.__dict__[attr]
        else:
            return getattr(self.__dict__['writer'], attr)
    
    def __setattr__(self, attr, val):
        if attr in self.__dict__ or attr in ('writer', 'unicode_deconverter', 'dict_deconverter',):
            self.__dict__[attr] = val
        else:
            setattr(self.__dict__['writer'], attr, val)

    
    # backwards compability
    def get_now(self): return self.writer.now
    def set_now(self, value): self.writer.now = value
    now = property(get_now, set_now)
        
