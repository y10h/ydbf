#!/usr/bin/env python
"""
Move DBF data (as single table) to RDBMS using SQLAlchemy
"""

import os
import ydbf
import sqlalchemy as sa


def _get_column_name(dbf_name):
    """
    Return name of column for specified dbf field name
    """
    # may make not only low name, but more complex stuff
    return dbf_name.lower()


def _get_table_name(dbf_name):
    """
    Return name of table for specified name of DBF file
    """
    return os.path.basename(dbf_name).lower().replace('.dbf', '')


def _create_column(dbf_name, dbf_type, dbf_length, dbf_decimal):
    """
    Return SQLAlchemy column for specified dbf field
    """
    if dbf_type == 'C':
        sa_type = sa.Unicode(dbf_length)
    elif dbf_type == 'D':
        sa_type = sa.Date
    elif dbf_type == 'N':
        if dbf_decimal == 0:
            sa_type = sa.Integer
        else:
            # WARN: all decimals are converted to float, may loose some info
            sa_type = sa.Float
    elif dbf_type == 'L':
        sa_type = sa.Boolean
    else:
        raise ValueError("Dosen't know how convert %r type" % dbf_type)
    return sa.Column(_get_column_name(dbf_name), sa_type)


def make_table(metadata, reader):
    """
    Construct table (create if it doesn't exist) from YDbfReader instance

    Arguments:

        ``metadata``
            SQLAlchemy MetaData isntance, *must* be binded to engine

        ``reader``
            YDbfReader instance
    """
    table_name = _get_table_name(reader.fh.name)
    columns = [_create_column(*field) for field in reader.fields]
    table = sa.Table(table_name, metadata, *columns)
    if not table.exists():
        table.create()
    return table


def convert_data(dbf_data_iterator):
    """
    Convert dbf data to SQLAlchemy
    """
    # may make not only simple name conversion, but more complex stuff
    for rec in dbf_data_iterator:
        yield dict((_get_column_name(k), v) for k, v in rec.items())


def __next_n(iterable, n=1):
    """
    Return next n items from iterable
    """
    res = []
    try:
        for i in range(n):
            res.append(iterable.next())
    except StopIteration:
        pass
    return res


def push_data(table, data_iterator, length=500):
    """
    Push data into table

    Arguments:

        ``table``
            SQLAlchemy Table instance, must be binded to engine

         ``data_iterator``
            iterator, each record is a dict with keys as table's column names

          ``length``
            insert N records in a single statement,
            by default 500
    """
    values = __next_n(data_iterator, length)
    while values:
        engine = table.bind
        engine.connect().execute(table.insert(), values)
        values = __next_n(data_iterator, length)


def make_meta(uri):
    """
    Make binded MetaData

    Arguments:
        ``uri``
            SQLAlchemy SB URI
    """
    engine = sa.create_engine(uri)
    return sa.MetaData(bind=engine)


def dbf2sa(dbf_name, sa_uri):
    """
    Move DBF data (as single table) from file to RDBMS

    Arguments:

        ``dbf_name``
            Full path to DBF file, move data from

        ``sa_uri``
            SQLAlchemy DB URI, move data to
    """
    reader = ydbf.open(dbf_name)
    meta = make_meta(sa_uri)
    table = make_table(meta, reader)
    push_data(table, convert_data(reader))


if __name__ == '__main__':
    import sys
    dbf2sa(sys.argv[1], 'sqlite:///converted.db')
