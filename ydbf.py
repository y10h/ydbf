# Name:        ydbf.py
# Purpose:     Yielded dbf reader/writer 
# based on idbf (http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/362715)
#
# Authors:     Yury Yurevich <python@yurevich.ru>
#              Alexandr Zamaraev <tonal@promsoft.ru> 
#
# Created:     2006/05/24
# Licence:     GNU GPL2

__revision__ = "$Id$"
__url__ = "$URL$"

import struct
import datetime
import itertools
import unittest

from cStringIO import StringIO


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
    return datetime.date(y, m, d)

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

class YDbfReader(object):
    '''
    Class for reading DBF
    '''
    def __init__(self, fh):
        '''
        Iterator ove DBF records
        @param fh: filehandler (should be opened for binary reads)
        '''
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
        
        self.dbf2date = dbf2date # function for conversion from dbf to date

        self.readHeader()

    def readHeader(self):
        '''
        Read DBF header
        ''' 
        self.fh.seek(0)
        numrec, lenheader = struct.unpack('<xxxxLH22x',
                                          self.fh.read(32))
        numfields = (lenheader - 33) // 32
        fields = []
        for fieldno in xrange(numfields):
            name, typ, size, deci = struct.unpack('<11sc4xBB14x',
                                                  self.fh.read(32))
            name = name.split('\0', 1)[0]       # NUL is a end of string
            fields.append((name, typ, size, deci))
        terminator = self.fh.read(1)
        assert terminator == '\r'
                
        fields.insert(0, ('DeletionFlag', 'C', 1, 0))
        self._fields = fields  # with DeletionFlag
        self.fields = fields[1:] # without DeletionFlag
        recfmt = ''.join(['%ds' % fld[2] for fld in fields])
        recsize = struct.calcsize(recfmt)
        self.numrec = numrec
        self.lenheader = lenheader
        self.numfields = numfields
        self.stop_at = numrec
        self.field_names = [fld[0] for fld in self.fields]
        self.recfmt = recfmt
        self.recsize = recsize
    
    def __len__(self):
        '''
        Get number of records in DBF
        @return: number of records (integer)
        '''
        return self.numrec
    
    def __call__(self, start_from=None, limit=None, raise_on_unknown_type=False):
        '''
        Get iterator
        @param start_from: index of record start from (optional)
        @param limit: limits number of iterated records (optional)
        @param raise_on_unknown_type: raise or not exception for unknown type
            of field
        @return: iterator over records
        '''
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
            raise ValueError("Unknown dbf-type: %s in field %s" % \
                    (self._fields[idx][1], self._fields[idx][0]))
        actions = {
            'D': lambda val: self.dbf2date(val.strip()),
            'L': lambda val: logic.get(val),
            'C': lambda val: val.strip(),
            'N': lambda val: int(val),
            'ND': lambda val: float(val),
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
                    raise RuntimeError("Error occured while reading value '%s' from field '%s' (rec #%d)" % \
                            (val, self._fields[idx][0], self.i))
            yield res

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


    class TestDateConverters(unittest.TestCase):
        
        def test_dbf2date(self):
            self.assertEqual(dbf2date(''), datetime.date(1900, 1, 1))
            self.assertEqual(dbf2date('None'), datetime.date(1900, 1, 1))
            self.assertEqual(dbf2date(None), datetime.date(1900, 1, 1))
            self.assertEqual(dbf2date('20060506'), datetime.date(2006, 5, 6))
            self.assertEqual(dbf2date('0'), datetime.date(2200, 1, 1))
            self.assertEqual(dbf2date('000'), datetime.date(2200, 1, 1))
        
        def test_date2dbf(self):
            self.assertEqual(date2dbf(datetime.date(2006, 5, 6)), '20060506')
            self.assertRaises(TypeError, date2dbf, None)
            self.assertRaises(TypeError, date2dbf, '20060506')
        
        def test_dbf2str(self):
            self.assertEqual(dbf2str(''), '01.01.1900')
            self.assertEqual(dbf2str('None'), '01.01.1900')
            self.assertEqual(dbf2str(None), '01.01.1900')
            self.assertEqual(dbf2str('20060506'), '06.05.2006')
            self.assertEqual(dbf2str('0'), '01.01.2200')
            self.assertEqual(dbf2str('000'), '01.01.2200')
        
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
        self.assertEqual(self.dbf.ver, 3)

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

        
if __name__ == '__main__':
    unittest.main()
