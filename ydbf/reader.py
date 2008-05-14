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
DBF reader
"""
__all__ = ["YDbfBasicReader", "YDbfStrictReader", "YDbfReader"]

import datetime
import struct
import itertools

from ydbf import lib

class YDbfBasicReader(object):
    """
    Basic class for reading DBF
    
    Instance is an iterator over DBF records
    
    @param fh: filehandler (should be opened for binary reads)
    """
    def __init__(self, fh):
        """
        Constructor
        """
        self.fh = fh             # filehandler
        self.numrec = 0          # number of records
        self.lenheader = 0       # length of header
        self.numfields = 0       # number of fields
        self.fields = []         # fields info in format [(NAME, TYP, SIZE, DEC),]
        self.field_names = ()    # field names (i.e. (NAME,))
        self.start_from = 0      # number of rec, iteration started from
        self.stop_at = 0         # number of rec, iteration stopped at (not include this)
        self.recfmt = ''         # struct-format of rec
        self.recsize = 0         # size of each record (in bytes)
        self.i = 0               # current item in iterator
        self.dt = None           # date of file creation
        self.dbf2date = lib.dbf2date # function for conversion from dbf to date

        self.readHeader()

    def readHeader(self):
        """
        Read DBF header
        """ 
        self.fh.seek(0)

        sig, year, month, day, numrec, lenheader, recsize, lang = struct.unpack(
            lib.HEADER_FORMAT,
            self.fh.read(32))
        year = year + 1900
        # some software use 0x08 as 2008 instead of 0x6c
        if year < 1950:
            year = year+100
        self.dt = datetime.date(year, month, day)
        self.sig = sig
        if sig not in lib.SUPPORTED_SIGNATURES:
            version = lib.SIGNATURES.get(sig, 'UNKNOWN')
            raise lib.DbfError("DBF version '%s' (signature %s) not supported" % (version, hex(sig)))
        
        numfields = (lenheader - 33) // 32
        fields = []
        for fieldno in xrange(numfields):
            name, typ, size, deci = struct.unpack(lib.FIELD_DESCRIPTION_FORMAT,
                                                  self.fh.read(32))
            name = name.split('\0', 1)[0]       # NULL is a end of string
            fields.append((name, typ, size, deci))
        terminator = self.fh.read(1)
        assert terminator == '\x0d', "Terminator must be 0x0d"
                
        fields.insert(0, ('DeletionFlag', 'C', 1, 0))
        self.raw_lang = lang
        self._fields = fields  # with DeletionFlag
        self.fields = fields[1:] # without DeletionFlag
        self.recfmt = ''.join(['%ds' % fld[2] for fld in fields])
        self.recsize = struct.calcsize(self.recfmt)
        self.numrec = numrec
        self.lenheader = lenheader
        self.numfields = numfields
        self.stop_at = numrec
        self.field_names = [fld[0] for fld in self.fields]
    
    def __len__(self):
        """
        Get number of records in DBF
        @return: number of records (integer)
        """
        return self.numrec
    
    def __call__(self, start_from=None, limit=None, raise_on_unknown_type=False):
        """
        Get iterator
        @param start_from: index of record start from (optional)
        @param limit: limits number of iterated records (optional)
        @param raise_on_unknown_type: raise or not exception for unknown type
            of field
        @return: iterator over records
        """
        if start_from is not None:
            self.start_from = start_from
        offset = self.lenheader + self.recsize*self.start_from
        if self.fh.tell() != offset:
            self.fh.seek(offset)
            
        if limit is not None:
            self.stop_at = self.start_from + limit

        logic = {
            'Y': True, 'y': True, 'T': True, 't': True,
            'N': False, 'n': False, 'F': False, 'f': False,
            }
        types_in_list = [t in ('N', 'D', 'L', 'C') for n, t, s, d in self._fields]
        if False in types_in_list and raise_on_unknown_type:
            idx = types_in_list.index(False)
            raise lib.DbfTypeError("Unknown dbf-type: %s in field %s" % \
                    (self._fields[idx][1], self._fields[idx][0]))
        actions = {
            'D': lambda val: self.dbf2date(val.strip()),
            'L': lambda val: logic.get(val.strip()),
            'C': lambda val: val.rstrip(),
            'N': lambda val: (val.strip() or 0) and int(val),
            'ND': lambda val: (val.strip() or 0.0) and float(val),
            'DeletionFlag': None,
            }
        converters = [
                         # first of all, is it DeletionFlag
            actions.get((name == 'DeletionFlag' and 'DeletionFlag') or
                         # secondary, D or C or someting else
                    (typ != 'N' and typ) or
                         # third, decimal (ND)
                    (deci and 'ND') or
                         # the last -- N
                    'N') # default value -- None
                   for name, typ, size, deci in self._fields]
        
        for i in xrange(self.start_from, self.stop_at):
            record = struct.unpack(self.recfmt, self.fh.read(self.recsize))
            self.i = i
            if record[0] != ' ':
                continue                        # deleted record
            try:
                res = [conv(val)
                    for idx, (val, conv) in enumerate(itertools.izip(record, converters))
                    if conv
                    ]
            except (IndexError, ValueError, TypeError), err:
                    raise lib.DbfError("Error occured while reading value '%s' from field '%s' (rec #%d)" % \
                            (val, self._fields[idx][0], self.i))
            yield res

class YDbfStrictReader(YDbfBasicReader):
    """
    DBF-reader with additional checks
    """
    def __init__(self, fh):
        """
        Create strict DBF-reader
        
        @param fh: filehandler (should be opened for binary reads)
        """
        super(YDbfStrictReader, self).__init__(fh)
        self.checkConsistency()

    def checkConsistency(self):
        """
        Some checks of DBF structure
        
        @raise AssertionError: if some check failed
        """
        ## check records
        assert self.recsize >1, "Length of record must be >1"
        if self.sig in (0x03, 0x04): 
            assert self.recsize < 4000, "Length of record must be <4000 B for dBASE III and IV"
        assert self.recsize < 32*1024, "Length of record must be <32KB"
        assert self.numrec >= 0, "Number of records must be non-negative"

        ## check fields
        assert self.numfields > 0, "The dbf file must have at least one field"
        if self.sig == 0x03:
            assert self.numfields < 128, "Number of fields in dBASE III must be <128"
        if self.sig == 0x04:
            assert self.numfields < 256, "Number of fields in dBASE IV must be <256"

        ## check fields, round 2
        for f_name, f_type, f_size, f_decimal in self.fields:
            if f_type == 'N':
                assert f_size < 20, "Size of numeral field must be <20 (field '%s', size %d)" % (f_name, f_size)
            if f_type == 'C':
                assert f_size < 255, "Size of numeral field must be <255 (field '%s', size %d)" % (f_name, f_size)
            if f_type == 'L':
                assert f_size == 1, "Size of logical field must be 1 (field '%s', size %d)" % (f_name, f_size)

        ## check size, if available
        file_name = getattr(self.fh, 'name', None)
        if file_name is not None:
            import os
            try:
                os_size = os.stat(file_name)[6]
            except OSError, msg:
                return
            dbf_size = long(self.lenheader + 1 + self.numrec*self.recsize)
            assert os_size == dbf_size

class UnicodeConverter(object):
    """
    Unicode converter which converts all strings to unicode,
    using lang code in DBF file, or implicitly specified encoding
    """
    def __init__(self, default_encoding='ascii', overwrite_encoding=False, raw_lang=0x00):
        """
        Create unicode converter for DBF reader
        
        @param default_encoding: default encoding (default ascii) for strings 
            (if lang code in DBF is not found)
        @type default_encoding: C{str}
        
        @param overwrite_encoding: overwrite lang code by default encoding, default False
        @type overwrite_encoding: C{boolean}
        
        @param raw_lang: lang code from DBF file, default 0x00 (i.e. absence of lang code)
        @type raw_lang: C{int}
        """
        if overwrite_encoding:
            self.encoding = default_encoding
        else:
            self.encoding = lib.ENCODINGS.get(raw_lang, default_encoding)

    def __call__(self, records_iterator):
        """
        Convert strings to unicode
        """
        provide_unicode = lambda x, enc: (isinstance(x, str) and unicode(x, enc)) or x
        encoding = self.encoding
        for record in records_iterator:
            yield [provide_unicode(x, encoding) for x in record]

class DictConverter(object):
    """
    Convert from list to dict each record.
    Must be the last converter in chain.
    
    @param fields_struct: structure of DBF file
    """
    def __init__(self, fields_struct):
        """
        Create dict converter
        """
        self.fields = fields_struct

    def __call__(self, records_iterator):
        """
        Convert to dict each record
        
        @param records_iterator: iterator over DBF records
        """
        fields = self.fields
        for record in records_iterator:
            record_dict = {}
            for (f_name, f_type, f_size, f_decimal), value in itertools.izip(fields, record):
                record_dict[f_name] = value
            yield record_dict
            
class YDbfReader(object):
    """
    Most common DBF reader
        @param fh: filehandler (should be opened for binary reads)
        
        @param use_unicode: use unicode instead of strings, default False
        @type use_unicode: C{boolean}
        
        @param default_encoding: default encoding for DBF file,
            default 'ascii',
            uses with C{use_unicode} option
        @type default_encoding: C{str}
        
        @param overwrite_encoding: overwrite encoding specified
            in DBF file (lang code) by default value 
            (see C{default_encoding} option), default False,
            uses with C{use_unicode} option
        @type overwrite_encoding: C{boolean}
        
        @param as_dict: represent each record as dict instead of list,
            defult False
        @type as_dict: C{boolean}
        
        @param strict: make some checks of internal DBF structure,
            default True
        @type strict: C{boolean}
    """
    
    def __init__(self, fh, **kwargs):
        """
        Create DBF reader
        """
        use_unicode = kwargs.get('use_unicode', False)
        default_encoding = kwargs.get('default_encoding', 'ascii')
        overwrite_encoding = kwargs.get('overwrite_encoding', False)
        as_dict = kwargs.get('as_dict', False)
        strict = kwargs.get('strict', True)

        if strict:
            self.reader = YDbfStrictReader(fh)
        else:
            self.reader = YDbfBasicReader(fh)

        null_converter = lambda xiter: (x for x in xiter)
        
        if use_unicode:
            self.unicode_converter = UnicodeConverter(default_encoding, overwrite_encoding, self.reader.raw_lang)
        else:
            self.unicode_converter = null_converter

        if as_dict:
            self.dict_converter = DictConverter(self.reader.fields)
        else:
            self.dict_converter = null_converter

    def __call__(self, *args, **kwargs):
        """
        Get iterator over DBF records
        """
        return self.dict_converter(self.unicode_converter(self.reader(*args, **kwargs)))

    def __len__(self): 
        return len(self.reader)

    def __getattr__(self, attr):
        if attr in self.__dict__ or attr in ('reader', 'unicode_converter', 'dict_converter',):
            return self.__dict__[attr]
        else:
            return getattr(self.__dict__['reader'], attr)
    
    def __setattr__(self, attr, val):
        if attr in self.__dict__ or attr in ('reader', 'unicode_converter', 'dict_converter',):
            self.__dict__[attr] = val
        else:
            setattr(self.__dict__['reader'], attr, val)

