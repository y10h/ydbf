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
DBF reader
"""
__all__ = ["YDbfStrictReader", "YDbfReader"]

import datetime
from struct import calcsize, unpack
from itertools import izip

from ydbf import lib

try:
    from decimal import Decimal
    decimal_enabled = True
except ImportError:
    Decimal = lambda x: float(x)
    decimal_enabled = False

class YDbfReader(object):
    """
    Basic class for reading DBF
    
    Instance is an iterator over DBF records
    """
    def __init__(self, fh, fields=None, use_unicode=True, encoding=None):
        """
        Iterator over DBF records
        
        Args:
            `fh`:
                filehandler (should be opened for binary reading)
            
            `fields`:
                force to use your own DBF fields structure instead of builtin.
                Fields structure is defined as [(NAME, TYP, SIZE, DEC),]
            
            `use_unicode`:
                convert all char fields to unicode. Use builtin
                encoding (formerly lang code from DBF file) or
                implicitly defined encoding via `encoding` arg.
            
            `encoding`:
                force usage of implicitly defined encoding
                instead of builtin one. By default None.
        """
        self.fh = fh             # filehandler
        self.implicit_encoding = encoding
        if fields:
            self._fields = [('_deletion_flag', 'C', 1, 0)] + list(fields)
            self.fields = list(fields)
            self.builtin_fields = []
            self.builtin__fields = []
        else:
            self._fields = []
            self.fields = []
            self.builtin_fields = []
            self.builtin__fields = []
        self.numrec = 0          # number of records
        self.lenheader = 0       # length of header
        self.numfields = 0       # number of fields
        
        self.fields = []
        self.field_names = ()    # field names (i.e. (NAME,))
        self.start_from = 0      # number of rec, iteration started from
        self.stop_at = 0         # number of rec, iteration stopped at
                                 # (not include this)
        self.recfmt = ''         # struct-format of rec
        self.recsize = 0         # size of each record (in bytes)
        self.dt = None           # date of file creation
        self.dbf2date = lib.dbf2date # function for conversion from dbf to date
        

        self.encoding = None
        self.builtin_encoding = None

        self.converters = {}
        self.action_resolvers = ()

        self.iterator = None

        self._readHeader()
        if use_unicode:
            self._defineEncoding()
        self._makeActions()
        self.postInit()

    def postInit(self):
        # place where children want to add their own post-init actions
        pass

    def _makeActions(self):
        def dbf2py_date(val, size, dec):
            return self.dbf2date(val)
        
        def dbf2py_logic(val, size, dec):
            return val.strip() in ("Y", "y", "T", "t")
        
        def dbf2py_unicode(val, size, dec):
            return val.decode(self.encoding).rstrip()
        
        def dbf2py_string(val, size, dec):
            return val.rstrip()
        
        def dbf2py_integer(val, size, dec):
            return (val.strip() or 0) and int(val.strip())
        
        def dbf2py_decimal(val, size, dec):
            return Decimal(('%%.%df'%dec) % float(val.strip() or 0.0))

        self.action_resolvers = (
            lambda typ, size, dec: (typ == 'C' and self.encoding) and \
                                    dbf2py_unicode,
            lambda typ, size, dec: (typ == 'C' and not self.encoding) and \
                                    dbf2py_string,
            lambda typ, size, dec: (typ == 'N' and dec) and dbf2py_decimal,
            lambda typ, size, dec: (typ == 'N' and not dec) and dbf2py_integer,
            lambda typ, size, dec: typ == 'D' and dbf2py_date,
            lambda typ, size, dec: typ == 'L' and dbf2py_logic,
        )
        for name, typ, size, dec in self._fields:
            for resolver in self.action_resolvers:
                action = resolver(typ, size, dec)
                if callable(action):
                    self.converters[name] = action
                    break
            if not action:
                raise ValueError("Cannot find dbf-to-python converter "
                                 "for field %s (type %s)" % (name, typ))
                    
    def _readHeader(self):
        """
        Read DBF header
        """
        self.fh.seek(0)

        sig, year, month, day, numrec, lenheader, recsize, lang = unpack(
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
            raise ValueError("DBF version '%s' (signature %s) not supported"
                             % (version, hex(sig)))
        
        numfields = (lenheader - 33) // 32
        fields = []
        for fieldno in xrange(numfields):
            name, typ, size, deci = unpack(lib.FIELD_DESCRIPTION_FORMAT,
                                                  self.fh.read(32))
            name = name.split('\0', 1)[0]       # NULL is a end of string
            if typ not in ('N', 'D', 'L', 'C'):
                raise ValueError("Unknown type %r on field %s" % (typ, name))
            fields.append((name, typ, size, deci))

        terminator = self.fh.read(1)
        if terminator != '\x0d':
            raise ValueError("Terminator should be 0x0d. Terminator is a "
                             "delimiter, which splits header and data "
                             "sections in file. By specification it should be "
                             "0x0d, but it '%s'. This may be as result of "
                             "corrupted file, non-DBF data or error in YDbf "
                             "library." % hex(terminator))
        fields.insert(0, ('_deletion_flag', 'C', 1, 0))
        self.builtin__fields = fields  # with _deletion_flag
        self.builtin_fields = fields[1:] # without _deletion_flag
        if not self.fields:
            self.fields = self.builtin_fields
            self._fields = self.builtin__fields	
        self.raw_lang = lang
        self.recfmt = ''.join(['%ds' % fld[2] for fld in self._fields])
        self.recsize = calcsize(self.recfmt)
        self.numrec = numrec
        self.lenheader = lenheader
        self.numfields = numfields
        self.stop_at = numrec
        self.field_names = [fld[0] for fld in self.fields]

    def _defineEncoding(self):
        self.builtin_encoding = lib.ENCODINGS.get(self.raw_lang, (None,))[0]
        if self.builtin_encoding is None and self.implicit_encoding is None:
            raise ValueError("Cannot resolve builtin lang code %s "
                             "to encoding and no option `encoding` "
                             "passed, but `use_unicode` are, so "
                             "there is no info how we can decode chars "
                             "to unicode. Please, set up option `encoding` "
                             "or set `use_unicode` to False"
                             % hex(self.raw_lang))
        if self.implicit_encoding:
            self.encoding = self.implicit_encoding
        else:
            self.encoding = self.builtin_encoding

    def __iter__(self):
        if not self.iterator:
            self.iterator = self.records()
        return self.iterator
    
    def next(self):
        if not self.iterator:
            self.iterator = self.records()
        return self.iterator.next()

    def __len__(self):
        """
        Get number of records in DBF
        """
        return self.numrec
    
    def records(self, start_from=None, limit=None, show_deleted=False):
        """
        Iterate over DBF records
        
        Args:
            `start_from`:
                index of record start from (optional)
            `limit`:
                limits number of iterated records (optional)
            `show_deleted`:
                do not skip deleted records (optional)
                False by default
        """
        
        if start_from is not None:
            self.start_from = start_from
        offset = self.lenheader + self.recsize*self.start_from
        if self.fh.tell() != offset:
            self.fh.seek(offset)
        
        if limit is not None:
            self.stop_at = self.start_from + limit

        converters = tuple((self.converters[name], name, size, dec)
                           for name, typ, size, dec in self._fields)
        for i in xrange(self.start_from, self.stop_at):
            record = unpack(self.recfmt, self.fh.read(self.recsize))
            if not show_deleted and record[0] != ' ':
                # deleted record
                continue
            try:
                yield dict((name, conv(val.rstrip('\x00'), size, dec))
                            for (conv, name, size, dec), val
                            in izip(converters, record)
                            if (name != '_deletion_flag' or show_deleted))
            except UnicodeDecodeError, err:
                args = list(err.args[:-1]) + [
                    "Error occured while reading rec #%d. You are "
                    "using YDbfReader with unicode-related options: "
                    "actual encoding %s, builtin DBF encoding %s (raw lang "
                    "code %s), manually set encoding is %s. Probably, data "
                    "in DBF file is not encoded with %s encoding, so you "
                    "should manually define encoding by setting up `encoding` "
                    "option" % (i, self.encoding, self.builtin_encoding,
                    hex(self.raw_lang), self.implicit_encoding, self.encoding)]
                raise UnicodeDecodeError(*args)
            except (IndexError, ValueError, TypeError, KeyError), err:
                raise RuntimeError("Error occured (%s: %s) while reading rec "
                                   "#%d" % (err.__class__.__name__, err, i))

    def read(self):
        return self.records()
    
    def close(self):
        return self.fh.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

class YDbfStrictReader(YDbfReader):
    """
    DBF-reader with additional logical checks
    """

    def postInit(self):
        super(YDbfStrictReader, self).postInit()
        self.checkConsistency()

    def checkConsistency(self):
        """
        Some logical checks of DBF structure.
        If some check failed, AssertionError is raised.
        """
        ## check records
        assert self.recsize > 1, "Length of record must be >1"
        if self.sig in (0x03, 0x04):
            assert self.recsize < 4000, "Length of record must be <4000 B " \
                                        "for dBASE III and IV"
        assert self.recsize < 32*1024, "Length of record must be <32KB"
        assert self.numrec >= 0, "Number of records must be non-negative"

        ## check fields
        assert self.numfields > 0, "The dbf file must have at least one field"
        if self.sig == 0x03:
            assert self.numfields < 128, "Number of fields in dBASE III " \
                                         "must be <128"
        if self.sig == 0x04:
            assert self.numfields < 256, "Number of fields in dBASE IV " \
                                         "must be <256"

        ## check fields, round 2
        for f_name, f_type, f_size, f_decimal in self.fields:
            if f_type == 'N':
                assert f_size < 20, "Size of numeral field must be <20 " \
                                    "(field '%s', size %d)" % (f_name, f_size)
            if f_type == 'C':
                assert f_size < 255, "Size of numeral field must be <255 " \
                                     "(field '%s', size %d)" % (f_name, f_size)
            if f_type == 'L':
                assert f_size == 1, "Size of logical field must be 1 (field " \
                                    "'%s', size %d)" % (f_name, f_size)

        ## check size, if available
        file_name = getattr(self.fh, 'name', None)
        if file_name is not None:
            import os
            try:
                os_size = os.stat(file_name)[6]
            except OSError:
                return
            dbf_size = long(self.lenheader + 1 + self.numrec*self.recsize)
            assert os_size == dbf_size, "Logical size (calculated from file " \
                                        "structure and number of records) " \
                                        "should be equal to size of file"

