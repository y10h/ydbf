#!/usr/bin/env python
# encoding: utf-8
"""
Create DBF file with random data.

May be useful for some testing.
"""

from __future__ import print_function

import datetime
import decimal
import random
import sys

import ydbf


def get_n_random(size, dec):
    assert size < 20, "number cannot be longer than 20 digits (got %s)" % size
    assert dec < size, "size (%s) must be bigger than dec (%s)" % (size, dec)
    n = random.randint(0, 10 ** (size - dec - 1) - 1)
    if dec:
        if size - dec - 1 == 0:
            dec = dec - 1
        decimal_part = [int(y) for y in str(random.randint(1, 10 ** (dec - 1)))]

        length = len(decimal_part)
        if length < dec:
            decimal_part += [0 for _ in range(dec - length)]
        signs = tuple([int(x) for x in str(n)] + decimal_part)
        darg = (0, signs, -dec)
        n = decimal.Decimal(darg)
    return n


def get_d_random(size, dec):
    try:
        return datetime.date(
            random.randint(1899, 2030), random.randint(1, 12), random.randint(1, 28)
        )
    except ValueError:
        return get_d_random(size, dec)


def get_c_random(size, dec, force_ascii=False):
    assert size < 255, "string cannot be longer than 255 (got %s)" % size
    alphabet = (
        {
            "consonants": (
                "b",
                "d",
                "g",
                "h",
                "k",
                "l",
                "m",
                "n",
                "p",
                "r",
                "s",
                "t",
                "z",
            ),
            "vowels": ("a", "e", "i", "o", ""),
        },
        # want some cyrillic for unicode ;)
        {
            "consonants": (
                "б",
                "д",
                "г",
                "х",
                "к",
                "л",
                "м",
                "н",
                "п",
                "р",
                "с",
                "т",
                "з",
            ),
            "vowels": ("а", "е", "и", "о", "у"),
        },
    )

    c = ""
    if force_ascii:
        alph = alphabet[0]
    else:
        alph = random.choice(alphabet)
    size = random.randint(int(size / 2), size)
    while len(c) < size:
        c += random.choice(alph["consonants"]) + random.choice(alph["vowels"])
        if random.choice((False, False, False, False, False, True)):
            c += " "
    if len(c) > size:
        c = c[:size]
    return c


def get_l_random(size, dec):
    assert size == 1
    return random.choice((True, False))


def get_rec(fields_struct):
    rec = {}
    for name, typ, size, dec in fields_struct:
        assert typ in (ydbf.CHAR, ydbf.DATE, ydbf.LOGICAL, ydbf.NUMERAL)
        getter = globals().get("get_%s_random" % typ.lower())
        if not callable(getter):
            raise ValueError(
                "Cannot get data getter for DBF type %s (field %s)" % (typ, name)
            )
        value = getter(size, dec)
        # 10% of records -- None
        rec[name] = random.choice([value for _ in range(9)] + [None])
    return rec


def get_field():
    size_limits = {
        ydbf.NUMERAL: (1, 19),
        ydbf.CHAR: (1, 254),
        ydbf.LOGICAL: (1, 1),
        ydbf.DATE: (8, 8),
    }
    name = str(get_c_random(11, 0, force_ascii=True).replace(" ", "_"))
    # most popular field type is a numeric
    # next string, next date and the last -- logical
    typ = random.choice(
        (
            ydbf.NUMERAL,
            ydbf.NUMERAL,
            ydbf.NUMERAL,
            ydbf.NUMERAL,
            ydbf.CHAR,
            ydbf.CHAR,
            ydbf.CHAR,
            ydbf.DATE,
            ydbf.DATE,
            ydbf.LOGICAL,
        )
    )
    size = random.randint(*size_limits[typ])
    dec = 0
    if typ == ydbf.NUMERAL and size > 6:
        rand_dec = random.randint(0, int(size / 2))
        dec = random.choice((0, rand_dec))
    return name, typ, size, dec


def get_fields_structure(fields_number):
    return tuple(get_field() for _ in range(fields_number))


def get_data(fields_structure, number_of_records):
    for _ in range(number_of_records):
        yield get_rec(fields_structure)


def gendbf(filename, number_of_records=2000, fields_number=20):
    fh = open(filename, "wb")
    fields = get_fields_structure(fields_number)
    dbf = ydbf.open(filename, ydbf.WRITE, fields, encoding="cp1251")
    dbf.write(get_data(fields, number_of_records))
    dbf.close()


def usage():
    print("Usage: gendbf.py <name> [number_of_records] [number_of_fields] ")
    print()
    sys.exit(1)


def main(args):
    nargs = len(args)
    if not args or nargs > 3:
        usage()
    call = [args[0]]
    if nargs >= 2:
        try:
            number_of_records = int(args[1])
            call.append(number_of_records)
        except ValueError:
            print("Err: '%s' is not valid integer for number_of_records" % args[1])
            usage()
    if nargs == 3:
        try:
            fields_number = int(args[2])
            call.append(fields_number)
        except ValueError:
            print("Err: '%s' is not valid integer for number_of_fields" % args[2])
            usage()
    gendbf(*call)


if __name__ == "__main__":
    main(sys.argv[1:])
