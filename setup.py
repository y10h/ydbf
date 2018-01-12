from setuptools import setup, find_packages
from os import path

version = '0.4'

README_PATH = path.join(path.dirname(path.abspath(__file__)), 'README.md')
try:
    import m2r
    LONG_DESCRIPTION = m2r.parse_from_file(README_PATH)
except Exception:
    LONG_DESCRIPTION = ''

setup(name='ydbf-py3',
      version=version,
      description="Pythonic reader and writer for DBF/XBase files",
      long_description="""\
YDbf is a library for reading/writing DBF files
(also known as XBase) in pythonic way. It
represents DBF file as data iterator, where
each record is a simple dict.
.""",
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License (GPL)',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.5',
          'Topic :: Database',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],
      keywords='',
      author='Scott Walton',
      author_email='s@cott.me.uk',
      url='http://www.pyobject.ru/projects/YDbf',
      license='GNU GPL2',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=True,
      install_requires=[
          'six'
      ],
      entry_points="""
      # -*- Entry points: -*-
      [console_scripts]
      ydbfdump = ydbf.dump:main
      """,
      )
