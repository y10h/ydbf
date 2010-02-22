# -*- coding: utf-8 -*-
# encoding: utf-8
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
Unit-tests for YDbf
"""

import datetime
import unittest
import tempfile
import decimal
import os
from StringIO import StringIO

from ydbf import YDbfReader, YDbfWriter
from ydbf.lib import date2dbf, str2dbf, dbf2date, dbf2str

def testdata(filename=None, mode='rb'):
    """
    Decorator for organizing test data files
    """
    skip = False
    if filename is None:
        # use temp file
        _, filepath = tempfile.mkstemp(suffix='.dbf')
    else:
        filepath = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                'testdata', filename))
        if not os.path.isfile(filepath):
            skip = True
    def testrunner(testmethod):
        def wrapper(self):
            if skip:
                print "test %s SKIPPED, have no test file %s" \
                      % (testmethod.__name__, filename)
                outp = None
            else:
                fh = open(filepath, mode)
                outp = testmethod(self, fh)
                fh.close()
                if filename is None:
                    # delete temp file after test
                    try:
                        os.unlink(filepath)
                    except OSError:
                        pass
            return outp
        return wrapper
    return testrunner

class TestDateConverters(unittest.TestCase):
        
    def test_dbf2date(self):
        self.assertEqual(dbf2date(''), None)
        self.assertEqual(dbf2date('None'), None)
        self.assertEqual(dbf2date(None), None)
        self.assertEqual(dbf2date('20060506'), datetime.date(2006, 5, 6))
        self.assertEqual(dbf2date('0'), None)
        self.assertEqual(dbf2date('000'), None)
        
    def test_date2dbf(self):
        self.assertEqual(date2dbf(datetime.date(2006, 5, 6)), '20060506')
        self.assertRaises(TypeError, date2dbf, None)
        self.assertRaises(TypeError, date2dbf, '20060506')
        
    def test_dbf2str(self):
        self.assertEqual(dbf2str(''), None)
        self.assertEqual(dbf2str('None'), None)
        self.assertEqual(dbf2str(None), None)
        self.assertEqual(dbf2str('20060506'), '06.05.2006')
        self.assertEqual(dbf2str('0'), None)
        self.assertEqual(dbf2str('000'), None)
        
    def test_str2dbf(self):
        self.assertEqual(str2dbf('06.05.2006'), '20060506')
        self.assertRaises(TypeError, str2dbf, None)
        self.assertRaises(TypeError, str2dbf, datetime.date(2200, 1, 1))
        self.assertRaises(ValueError, str2dbf, '')
        self.assertRaises(ValueError, str2dbf, '06/05/2006')

    def test_olddates(self):
        """
        Check that we can convert dates older than 1901
        """
        self.assertEqual(dbf2date('18990506'), datetime.date(1899, 5, 6))
        self.assertEqual(date2dbf(datetime.date(1899, 5, 6)), '18990506')
        self.assertEqual(str2dbf('06.05.1899'), '18990506')
        self.assertEqual(dbf2str('18990506'), '06.05.1899')
        

class TestYDbfReader(unittest.TestCase):
    
    @testdata('simple.dbf')
    def test_constructor(self, fh):
        """
        Unit-test for reader's constructor
        """
        dbf_data = fh.read()
        self.assertEquals(YDbfReader(StringIO(dbf_data)).raw_lang,
                          0)
        self.assertEquals(YDbfReader(StringIO(dbf_data), use_unicode=True
                                    ).raw_lang,
                          0)
        self.assertEquals(YDbfReader(StringIO(dbf_data), use_unicode=True
                                    ).encoding,
                          'ascii')
        self.assertEquals(YDbfReader(StringIO(dbf_data), use_unicode=False
                                    ).encoding,
                          None)
        # without unicode encoding means nothing
        self.assertEquals(YDbfReader(StringIO(dbf_data), use_unicode=False,
                                     encoding='cp866').encoding,
                          None)
        self.assertEquals(YDbfReader(StringIO(dbf_data), use_unicode=True,
                                     encoding='cp866').encoding,
                          'cp866')
    
    @testdata('simple.dbf')
    def test_dbf2date(self, fh):
        dbf = YDbfReader(fh)
        self.assertEqual(dbf.dbf2date, dbf2date)

    @testdata('simple.dbf')
    def test_header(self, fh):
        dbf = YDbfReader(fh)
        self.assertEqual(dbf._fields, [('_deletion_flag', 'C', 1, 0),
                                        ('INT_FLD',      'N', 4, 0),
                                        ('FLT_FLD',      'N', 5, 2),
                                        ('CHR_FLD',      'C', 6, 0),
                                        ('DTE_FLD',      'D', 8, 0),
                                        ('BLN_FLD',      'L', 1, 0)])
        self.assertEqual(dbf.fields, [('INT_FLD',      'N', 4, 0),
                                       ('FLT_FLD',      'N', 5, 2),
                                       ('CHR_FLD',      'C', 6, 0),
                                       ('DTE_FLD',      'D', 8, 0),
                                       ('BLN_FLD',      'L', 1, 0)])
        self.assertEqual(dbf.numrec, 3)
        self.assertEqual(dbf.stop_at, 3)
        self.assertEqual(dbf.lenheader, 193)
        self.assertEqual(dbf.numfields, 5)
        self.assertEqual(dbf.recsize, 25)
        self.assertEqual(dbf.recfmt, '1s4s5s6s8s1s')
        self.assertEqual(dbf.field_names, ['INT_FLD', 'FLT_FLD',
                                            'CHR_FLD', 'DTE_FLD',
                                            'BLN_FLD'])
    
    @testdata('simple.dbf')
    def test_len(self, fh):
        dbf = YDbfReader(fh)
        self.assertEqual(len(dbf), 3)

    @testdata('simple.dbf')
    def test_call(self, fh):
        dbf = YDbfReader(fh)
        reference_data = [{'INT_FLD': 25,
                           'FLT_FLD': decimal.Decimal('12.34'),
                           'CHR_FLD': u'test',
                           'DTE_FLD': datetime.date(2006,  5,  7),
                           'BLN_FLD': True},
                          {'INT_FLD': 113,
                           'FLT_FLD': decimal.Decimal('1.01'),
                           'CHR_FLD': u'del',
                           'DTE_FLD': datetime.date(2006, 12, 23),
                           'BLN_FLD': False},
                               # skipped deleted line
                         ]
        self.assertEqual(list(dbf), reference_data)
        self.assertEqual(list(dbf.records(start_from=1)),
                         [reference_data[1]])
        self.assertEqual(list(dbf.records(start_from=0, limit=1)),
                         [reference_data[0]])

    @testdata('ooonumbug.dbf')
    def test_ooo_num_bug(self, fh):
        # OpenOffice produces wrong dbfs: it justify numbers on left and fill
        # it zeros (0x00)
        dbf = YDbfReader(fh)
        reference_data = [{'INT_FLD': 25,
                           'FLT_FLD': decimal.Decimal('12.34'),
                           'CHR_FLD': u'test',
                           'DTE_FLD': datetime.date(2006,  5,  7),
                           'BLN_FLD': True},
                          {'INT_FLD': 113,
                           'FLT_FLD': decimal.Decimal('1.01'),
                           'CHR_FLD': u'del',
                           'DTE_FLD': datetime.date(2006, 12, 23),
                           'BLN_FLD': False},
                         ]
        self.assertEqual(list(dbf), reference_data)
        self.assertEqual(list(dbf.records(start_from=1)),
                         [reference_data[1]])
        self.assertEqual(list(dbf.records(start_from=0, limit=1)),
                         [reference_data[0]])

    @testdata('simple.dbf')
    def test_read_deleted(self, fh):
        dbf = YDbfReader(fh)
        reference_data = [{'_deletion_flag': u'',
                           'INT_FLD': 25,
                           'FLT_FLD': decimal.Decimal('12.34'),
                           'CHR_FLD': u'test',
                           'DTE_FLD': datetime.date(2006,  5,  7),
                           'BLN_FLD': True},
                          {'_deletion_flag': u'',
                           'INT_FLD': 113,
                           'FLT_FLD': decimal.Decimal('1.01'),
                           'CHR_FLD': u'del',
                           'DTE_FLD': datetime.date(2006, 12, 23),
                           'BLN_FLD': False},
                          {'_deletion_flag': u'*',
                           'INT_FLD': 7436,
                           'FLT_FLD': decimal.Decimal('0.50'),
                           'CHR_FLD': u'ex.',
                           'DTE_FLD': datetime.date(2006, 7, 15),
                           'BLN_FLD': True},
                         ]
        self.assertEqual(list(dbf.records(show_deleted=True)), reference_data)

    @testdata('wrongtype.dbf')
    def test_wrongtype(self, fh):
        self.assertRaises(ValueError, YDbfReader, fh)

class TestReaderConverters(unittest.TestCase):

    @testdata('simple.dbf')
    def setUp(self, fh):
        self.dbf = YDbfReader(fh)
        self.sizes = {}
        for name, typ, size, dec in self.dbf.fields:
            self.sizes[name] = size, dec
    
    def _getConv(self, name):
        size, dec = self.sizes[name]
        return lambda x: self.dbf.converters[name](x, size, dec)
    
    def test_int(self):
        conv = self._getConv('INT_FLD') 
        self.assertEquals(conv('    '), 0)
        self.assertEquals(conv('   0'), 0)
        self.assertEquals(conv(' 0  '), 0)
        self.assertEquals(conv(' 100'), 100)
        self.assertEquals(conv('3452'), 3452)
        self.assertRaises(ValueError, conv, 'foo')

    def test_decimal(self):
        conv = self._getConv('FLT_FLD')
        self.assertEquals(conv('     '), decimal.Decimal('0.00'))
        self.assertEquals(conv(' 0   '), decimal.Decimal('0.00'))
        self.assertEquals(conv('    5'), decimal.Decimal('5.00'))
        self.assertEquals(conv(' 5.2 '), decimal.Decimal('5.20'))
        self.assertEquals(conv(' 5.30'), decimal.Decimal('5.30'))
        self.assertEquals(conv('12.34'), decimal.Decimal('12.34'))
        self.assertRaises(ValueError, conv, 'foo')

    def test_char_unicode(self):
        conv = self._getConv('CHR_FLD')
        self.assertEquals(conv('      '), u'')
        self.assertEquals(conv('  x   '), u'  x')
        self.assertEquals(conv('x     '), u'x')
        self.assertRaises(UnicodeDecodeError, conv, '\xf2\xe5\xf1\xf2')

    def test_date(self):
        conv = self._getConv('DTE_FLD')
        self.assertEquals(conv('20090727'), datetime.date(2009, 7, 27))
        self.assertEquals(conv('18990302'), datetime.date(1899, 3, 2))
        self.assertEquals(conv('        '), None)
        self.assertEquals(conv('foo'), None) #XXX  may be raise ValueError?

    def test_boolean(self):
        conv = self._getConv('BLN_FLD')
        self.assertEquals(conv('t'), True)
        self.assertEquals(conv('T'), True)
        self.assertEquals(conv('y'), True)
        self.assertEquals(conv('f'), False)
        self.assertEquals(conv('F'), False)
        self.assertEquals(conv('n'), False)
        self.assertEquals(conv('N'), False)
        # some that is not yYtT is False
        self.assertEquals(conv(' '), False)
        self.assertEquals(conv('x'), False)

class TestReaderNonunicodeConverters(unittest.TestCase):
    
    @testdata('simple.dbf')
    def setUp(self, fh):
        self.dbf = YDbfReader(fh, use_unicode=False)
        self.sizes = {}
        for name, typ, size, dec in self.dbf.fields:
            self.sizes[name] = size, dec

    def _getConv(self, name):
        size, dec = self.sizes[name]
        return lambda x: self.dbf.converters[name](x, size, dec)

    def test_char_8bit(self):
        conv = self._getConv('CHR_FLD')
        self.assertEquals(conv('      '), '')
        self.assertEquals(conv('  x   '), '  x')
        self.assertEquals(conv('x     '), 'x')
        self.assertEquals(conv('\xf2\xe5\xf1\xf2'), '\xf2\xe5\xf1\xf2')


class TestYdbfWriter(unittest.TestCase):
    def setUp(self):
        self.dbf_reference_data = '\x03j\x06\x13\x03\x00\x00\x00\xc1\x00\x19\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00INT_FLD\x00\x00\x00\x00N\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00FLT_FLD\x00\x00\x00\x00N\x00\x00\x00\x00\x05\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00CHR_FLD\x00\x00\x00\x00C\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00DTE_FLD\x00\x00\x00\x00D\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00BLN_FLD\x00\x00\x00\x00L\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\r   2512.34test  20060507T  113 1.01del   20061223F 7436 0.50ex.   20060715T\x1a'

        self.reference_data = [
        {'INT_FLD':  25, 'FLT_FLD':12.34, 'CHR_FLD':'test',
         'DTE_FLD':datetime.date(2006,  5,  7), 'BLN_FLD':True},
        {'INT_FLD': 113, 'FLT_FLD': 1.01, 'CHR_FLD': 'del',
         'DTE_FLD':datetime.date(2006, 12, 23), 'BLN_FLD':False},
        {'INT_FLD':7436, 'FLT_FLD': 0.5,  'CHR_FLD': 'ex.',
         'DTE_FLD':datetime.date(2006,  7, 15), 'BLN_FLD':True},
        ]
        self.fields = [('INT_FLD',      'N', 4, 0),
                       ('FLT_FLD',      'N', 5, 2),
                       ('CHR_FLD',      'C', 6, 0),
                       ('DTE_FLD',      'D', 8, 0),
                       ('BLN_FLD',      'L', 1, 0)]
        self.fh = StringIO()
        self.dbf = YDbfWriter(self.fh, self.fields)
    
    def test_header(self):
        self.assertEqual(self.dbf.now, datetime.date.today())
        self.assertEqual(self.dbf.numrec, 0)
        self.assertEqual(self.dbf.lenheader, 193)
        self.assertEqual(self.dbf.recsize, 25)
        self.assertEqual(self.dbf.numfields, 5)
        self.assertEqual(self.dbf.sig, 3)

    def test_date2dbf(self):
        self.assertEqual(self.dbf.date2dbf, date2dbf)
       
    def test_write(self):
        self.dbf.now = datetime.date(2006, 6, 19)
        self.assertEqual(self.dbf.now, datetime.date(2006, 6, 19))
        self.dbf.write(self.reference_data)
        self.assertEqual(self.dbf.numrec, 3)
        self.fh.seek(0)
        data = self.fh.read()
        self.assertEqual(data, self.dbf_reference_data)

    def test_wrongtype(self):
        fields = (
            ('INT_FLD', 'N', 4, 0),
            ('FLT_FLD' , 'N', 5, 2),
            ('WRNG_FLD', '\xd1', 6, 0),
            ('DTE_FLD', 'D', 8, 0),
            ('BLN_FLD', 'L', 1, 0),
        )
        fh = StringIO("")
        self.assertRaises(ValueError, YDbfWriter, fh, fields)


if __name__ == '__main__':
    unittest.main()
