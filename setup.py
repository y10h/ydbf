from setuptools import setup, find_packages
import sys, os

_README = os.path.join(os.path.dirname(__file__), "README.md")
with open(_README) as fh:
    _LONG_DESCRIPTION = fh.read()

version = "0.5rc"

setup(
    name="YDbf",
    version=version,
    description="Pythonic reader and writer for DBF/XBase files",
    long_description=_LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="",
    author="Yury Yurevich",
    author_email="python@y10h.com",
    url="https://github.com/y10h/ydbf",
    license="BSD",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    include_package_data=True,
    zip_safe=True,
    install_requires=[],
    entry_points="""
      # -*- Entry points: -*-
      [console_scripts]
      ydbfdump = ydbf.dump:main
      """,
)
