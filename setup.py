from setuptools import setup, find_packages
import sys, os

version = '0.0.1'

setup(name='YDbf',
      version=version,
      description="Yielded Dbf reader and writer",
      long_description="""\
Pythonic Dbf reader and writer""",
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
      install_requires=[],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )
      
