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
DBF writer
"""
__all__ = ["YDbfWriter"]

import struct
import datetime

from ydbf import lib

class YDbfWriter(object):
    """
    Writes DBF from iterator
    """
    def __init__(self, fh, fields, use_unicode=True, encoding='ascii'):
        """
        Create DBF writer
        
        Args:
            `fh`:
                filehandler, should be opened for binary writing
            `fields`:
                fields structure in format
                
                    [(NAME, TYP, SIZE, DEC), ...]
                
                where `NAME` is name of field (should be string),
                `TYP` is a DBF type (string from ("N", "D", "C", "L")),
                `SIZE` is a length of field (integer) and `DEC` -- length
                of decimal part (number of digits after the point). `SIZE`
                should include `DEC`.
            `use_unicode`:
                use unicode mode (traiting all string data as unicode) or not,
                default is True
            `encoding`:
                set encoding (lang code internally) of DBF file. If you are
                use unicode (recommended), then unicode data will be encoded
                by this encoding, else data will be written as is.
                Default is 'ascii', which means 0x00 lang code.
        """
        self.fh = fh
        self.fields = fields
        self.encoding = encoding
        self.use_unicode = use_unicode
        
        self.now = datetime.date.today()
        self.numrec = 0
        self.numfields = len(fields)
        self.lenheader = self.numfields * 32 + 33
        self.recsize = sum([field[2] for field in fields]) + 1
        self.date2dbf = lib.date2dbf
        self.sig = 0x03  # signature, DBF 3
        self.lang = 0x0 # default -- ascii, 0x00
        
        self.converters = {}
        self.action_resolvers = ()

        self._defineLangCode()        
        self._writeHeader()
        self._makeActions()        
    
    def _defineLangCode(self):
        lang_code = lib.REVERSE_ENCODINGS.get(self.encoding)
        encodings = ', '.join(sorted(lib.REVERSE_ENCODINGS.keys()))
        if not lang_code:
            raise ValueError("Encoding %s is not available for DBF, please "
                             "use one of: %s" % (self.encoding, encodings))
        self.lang = lang_code[0]

    def _makeActions(self):
        def py2dbf_date(val, size, dec):
            return (val and self.date2dbf(val)) or '        '
        
        def py2dbf_logic(val, size, dec):
            return (val and 'T') or 'F'
        
        def py2dbf_unicode(val, size, dec):
            return (val and val[:size].encode(self.encoding).ljust(size)) or \
                   ' '*size
        
        def py2dbf_string(val, size, dec):
            return (val and str(val)[:size].ljust(size)) or ' '*size
        
        def py2dbf_integer(val, size, dec):
            return ((val and str(val)) or '0').rjust(size)
        
        def py2dbf_decimal(val, size, dec):
            return ( (val and ("%%.%df"%dec) % float(str(val))) or \
                     '0.%s'%('0'*dec)
                   ).rjust(size)
        
        self.action_resolvers = (
            lambda typ, size, dec: (typ == 'C' and self.use_unicode) and \
                                   py2dbf_unicode,
            lambda typ, size, dec: (typ == 'C' and not self.use_unicode) and \
                                   py2dbf_string,
            lambda typ, size, dec: (typ == 'N' and dec) and py2dbf_decimal,
            lambda typ, size, dec: (typ == 'N' and not dec) and py2dbf_integer,
            lambda typ, size, dec: typ == 'D' and py2dbf_date,
            lambda typ, size, dec: typ == 'L' and py2dbf_logic,
        )
        for name, typ, size, dec in self.fields:
            for resolver in self.action_resolvers:
                action = resolver(typ, size, dec)
                if callable(action):
                    self.converters[name] = action
                    break
            if not action:
                raise ValueError("Cannot find python-to-dbf converter "
                                 "for field %s (type %s)" % (name, typ))        
        

    def _writeHeader(self):
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
            if typ not in ('N', 'D', 'L', 'C'):
                raise ValueError("Unknown type %r on field %s" % (typ, name))
            name = name.ljust(11, '\x00')
            fld = struct.pack(lib.FIELD_DESCRIPTION_FORMAT,
                              name, typ, size, deci)
            self.fh.write(fld)
        # terminator
        self.fh.write('\x0d')
        if pos > 0:
            self.fh.seek(pos)

    def flush(self):
        self._writeHeader()
        self.fh.flush()    
    
    def close(self):
        self.fh.close()    
    
    def write(self, records):
        """
        Run DBF-creator
        
        Args:
            `records`:
                iterator over records (each record is a dict of values)
        """
        i = 0
        for rec in records:
            i += 1
            try:
                raw_rec = ''.join(self.converters[name](rec[name], size, dec)
                                  for name, typ, size, dec in self.fields)
            except UnicodeDecodeError, err:
                self.flush()
                if self.use_unicode:
                    msg = "Error occured while writing rec #%d. You are "
                    "using YDbfWriter with unicode mode turned on (encoding "
                    "set to %s, lang code %s), but probably push 8-bit string "
                    "data to writer. Check yourself, please. Record data: "
                    "%s " % (i, self.encoding, hex(self.lang), rec)
                else:
                    msg = "Error occured while writing rec #%d. You are "
                    "using YDbfWriter with unicode mode turned off, so "
                    "we doesn't know why it occurs, so may be it is an "
                    "issue inside ydbf, or corrupted data, or some flowing "
                    "bug in your code. Check record data: %s" % (i, rec)
                args = list(err.args[:-1]) + [msg]
                raise UnicodeDecodeError(*args)
            except UnicodeEncodeError, err:
                self.flush()            
                if self.use_unicode:
                    msg = "Error occured while writing rec #%d. You are "
                    "using YDbfWriter with unicode mode turned on and encoding "
                    "%s (lang code %s). Probably, data you are pushing to "
                    "writer doesn't fit to %s encoding, please choose "
                    "another encoding (recommended), or encode your data "
                    "yourself and turn off unicode mode for writer. Record "
                    "data: %s" % (i, self.encoding, hex(self.lang),
                    self.encoding, rec)
                else:
                    msg = "Error occured while writing rec #%d. You are "
                    "using YDbfWriter with unicode mode turned off, but "
                    "probably push unicode data to writer. Check yourself, "
                    "please. Record data: %s " % (i, rec)
                args = list(err.args[:-1]) + [msg]
                raise UnicodeEncodeError(*args)
            except (IndexError, ValueError, TypeError, KeyError), err:
                self.flush()            
                raise RuntimeError("Error occured (%s: %s) while reading "
                                   "rec #%d. Record data: %s" %
                                   (err.__class__.__name__, err, i, rec))
            # first empty symbol is a deletion flag
            self.fh.write(' '+raw_rec)
            self.numrec = i
            if divmod(i, 1000)[1] == 0:
                # each 1k records flush header
                self.flush()
        self._writeHeader()
        # End of file
        self.fh.write('\x1A')
        self.fh.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
