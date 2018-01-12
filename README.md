## YDBF

Read/write DBF (DBase/XBase) files from Python. This is a pure-Python
implementation with no external dependencies.

## Quickstart

### Reading

The entrypoint of YDbf is an `open` function:

```python
dbf = ydbf.open('simple.dbf')
```

You can use file name, or already opened (in binary mode) file:

```python
with ydbf.open('simple.dbf') as dbf:
    for record in dbf:
        ...
```

Each record is represented as `dict`, where keys are names of fields.

### Writing

By default, YDbf opens file for reading, but you may set option `mode` to
open for writing:

```python
with ydbf.open('simple.dbf', 'w', fields) as dbf:
    dbf.write(data)
```

Or pass in a file handle:

```python
with open('simple.dbf', 'wb') as fh:
    dbf = ydbf.open(fh, 'w', fields)
```

`fields` is a structure description of DBF file, it is required option for
writing mode. Structure defined as sequence of field descriptions,
where each fields described by tuple `(NAME, TYP, SIZE, DEC)`. Where `NAME`
is a name of field, `TYP` -- `DBF` type of field (`'N'` for number, `'C'` for
char, `'D'` for date, `'L'` for logical), DEC is a precision (useful for `'N'`
type only). For example, like this:

```python
fields = [
    ('ID',      'N',  4, 0),
    ('VALUE',   'C', 40, 0),
    ('UPDATE',  'D', 8, 0),
    ('VISIBLE', 'L', 1, 0),
]
```

YDbf uses unicode for `'C'` fields by default, so you may want to define
encoding which be used for DBF file. Sadly, UTF-8 is not available, only
old scrappy 8-bits.

```python
dbf = ydbf.open('simple.dbf', 'w', fields, encoding='cp1251')
dbf.write(data)
```

YDbf gets `data` as iterator where each item is a dict, where
keys are name of fields. For early defined example it may looks like

```python
data = [
    {'ID': 1, 'VALUE': 'ydbf', 'VISIBLE': True,
        'UPDATE': datetime.date(2009, 7, 14)},
    {'ID': 2, 'VALUE': 'ydbf-dev', 'VISIBLE': False,
        'UPDATE': datetime.date(2009, 5, 15)},
    {'ID': 3, 'VALUE': 'pytils', 'VISIBLE': True,
        'UPDATE': datetime.date(2009, 5, 11)},
]
```
