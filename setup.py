from setuptools import setup, find_packages

version = '0.4'

setup(name='YDbf',
      version=version,
      description="Pythonic reader and writer for DBF/XBase files",
      long_description="""\
YDbf is a library for reading/writing DBF files
(also known as XBase) in pythonic way. It
represents DBF file as data iterator, where
each record is a simple dict.
.""",
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License (GPL)',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Database',
          'Topic :: Software Development :: Libraries :: Python Modules',
      ],
      keywords='',
      author='Yury Yurevich',
      author_email='the.pythy@gmail.com',
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
