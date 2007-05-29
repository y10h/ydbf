from setuptools import setup, find_packages
import sys, os

version = '0.0.1'

setup(name='YDbf',
      version=version,
      description="Yielded Dbf reader and writer",
      long_description="""\
Pythonic Dbf reader and writer""",
      classifiers=[], # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Yury Yurevich',
      author_email='the.pythy@gmail.com',
      url='http://pyobject.ru/projects/ydbf',
      license='GNU GPL2',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=True,
      install_requires=[],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
      
