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
Unit-tests for YDbf
"""

import datetime
import unittest
from cStringIO import StringIO

from ydbf import YDbfReader, YDbfWriter
from ydbf.lib import date2dbf, str2dbf, dbf2date, dbf2str

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


class TestYdbfReader(unittest.TestCase):

    def setUp(self):
        self.dbf_data = '\x03\x06\x06\x13\x03\x00\x00\x00\xc1\x00\x19\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00INT_FLD\x00312N\x05\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00FLT_FLD\x00\x00\x00\x00N\n\x00\x00\x00\x05\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00CHR_FLD\x00\x00\x00\x00C\x10\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00DTE_FLD\x00\x00\x00\x00D\x18\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00BLN_FLD\x00\x00\x00\x00L\x19\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\r   2512.34test  20060507T  113 1.01del   20061223F*7436 0.50ex.   20060715T\x1a'

        self.reference_data = [[25, 12.34, 'test',  datetime.date(2006,  5,  7),  True],
                               [113, 1.01,  'del',  datetime.date(2006, 12, 23), False],
                               # skipped deleted line
                              ]
        self.fh = StringIO(self.dbf_data)
        self.dbf = YDbfReader(self.fh)

    def test_dbf2date(self):
        self.assertEqual(self.dbf.dbf2date, dbf2date)

    def test_header(self):
        self.assertEqual(self.dbf._fields, [('DeletionFlag', 'C', 1, 0),
                                        ('INT_FLD',      'N', 4, 0),
                                        ('FLT_FLD',      'N', 5, 2),
                                        ('CHR_FLD',      'C', 6, 0),
                                        ('DTE_FLD',      'D', 8, 0),
                                        ('BLN_FLD',      'L', 1, 0)])
        self.assertEqual(self.dbf.fields, [('INT_FLD',      'N', 4, 0),
                                       ('FLT_FLD',      'N', 5, 2),
                                       ('CHR_FLD',      'C', 6, 0),
                                       ('DTE_FLD',      'D', 8, 0),
                                       ('BLN_FLD',      'L', 1, 0)])
        self.assertEqual(self.dbf.numrec, 3)
        self.assertEqual(self.dbf.stop_at, 3)
        self.assertEqual(self.dbf.lenheader, 193)
        self.assertEqual(self.dbf.numfields, 5)
        self.assertEqual(self.dbf.recsize, 25)
        self.assertEqual(self.dbf.recfmt, '1s4s5s6s8s1s')
        self.assertEqual(self.dbf.field_names, ['INT_FLD', 'FLT_FLD',
                                            'CHR_FLD', 'DTE_FLD',
                                            'BLN_FLD'])

    def test_len(self):
        self.assertEqual(len(self.dbf), 3)

    def test_call(self):
        self.assertEqual(list(self.dbf()), self.reference_data)
        self.assertEqual(list(self.dbf(start_from=1)),
                         [self.reference_data[1]])
        self.assertEqual(list(self.dbf(start_from=0, limit=1)),
                         [self.reference_data[0]])


class TestYdbfWriter(unittest.TestCase):
    def setUp(self):
        self.dbf_reference_data = '\x03j\x06\x13\x03\x00\x00\x00\xc1\x00\x19\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00INT_FLD\x00\x00\x00\x00N\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00FLT_FLD\x00\x00\x00\x00N\x00\x00\x00\x00\x05\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00CHR_FLD\x00\x00\x00\x00C\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00DTE_FLD\x00\x00\x00\x00D\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00BLN_FLD\x00\x00\x00\x00L\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\r   2512.34test  20060507T  113 1.01del   20061223F 7436 0.50ex.   20060715T\x1a'
        self.reference_data = [[25, 12.34, 'test', datetime.date(2006,  5,  7),  True],
                               [113, 1.01,  'del', datetime.date(2006, 12, 23), False],
                               [7436, 0.5,  'ex.', datetime.date(2006,  7, 15),  True],
                              ]
        self.reference_data_dict = [
        {'INT_FLD':  25, 'FLT_FLD':12.34, 'CHR_FLD':'test', 'DTE_FLD':datetime.date(2006,  5,  7), 'BLN_FLD':True},
        {'INT_FLD': 113, 'FLT_FLD': 1.01, 'CHR_FLD': 'del', 'DTE_FLD':datetime.date(2006, 12, 23), 'BLN_FLD':False},
        {'INT_FLD':7436, 'FLT_FLD': 0.5,  'CHR_FLD': 'ex.', 'DTE_FLD':datetime.date(2006,  7, 15), 'BLN_FLD':True},
        ]
        self.fields = [('INT_FLD',      'N', 4, 0),
                       ('FLT_FLD',      'N', 5, 2),
                       ('CHR_FLD',      'C', 6, 0),
                       ('DTE_FLD',      'D', 8, 0),
                       ('BLN_FLD',      'L', 1, 0)]
        self.fh = StringIO()
        self.dbf = YDbfWriter(self.fh, self.fields)
        self.dbf_dict = YDbfWriter(self.fh, self.fields, as_dict=True)
        
    
    def test_header(self):
        self.assertEqual(self.dbf.now, datetime.date.today())
        self.assertEqual(self.dbf.numrec, 0)
        self.assertEqual(self.dbf.lenheader, 193)
        self.assertEqual(self.dbf.recsize, 25)
        self.assertEqual(self.dbf.numfields, 5)
        self.assertEqual(self.dbf.sig, 3)

    def test_date2dbf(self):
        self.assertEqual(self.dbf.date2dbf, date2dbf)
    
    def test_call(self):
        self.dbf.now = datetime.date(2006, 6, 19)
        self.assertEqual(self.dbf.now, datetime.date(2006, 6, 19))
        self.dbf(self.reference_data)
        self.assertEqual(self.dbf.numrec, 3)
        self.fh.seek(0)
        data = self.fh.read()
        self.assertEqual(data, self.dbf_reference_data)
    
    def test_call_as_dict(self):
        self.dbf_dict.now = datetime.date(2006, 6, 19)
        self.assertEqual(self.dbf_dict.now, datetime.date(2006, 6, 19))
        self.dbf_dict(self.reference_data_dict)
        self.assertEqual(self.dbf_dict.numrec, 3)
        self.fh.seek(0)
        data = self.fh.read()
        self.assertEqual(data, self.dbf_reference_data)

if __name__ == '__main__':
    unittest.main()
