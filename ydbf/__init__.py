# -*- coding: utf-8 -*-
# YDbf - Yielded dbf reader-writer
# Inspired by code of Raymond Hettinger
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/362715
#
# Copyright (C) 2006-2009 Yury Yurevich and contributors
#
# http://pyobject.ru/projects/ydbf/
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
YDbf reader-writer
"""
try:
    import pkg_resources
    try:
        VERSION = pkg_resources.get_distribution('YDbf').version
    except pkg_resources.DistributionNotFound:
        VERSION = 'unknown'
except ImportError:
    VERSION = 'N/A'
    
from ydbf.reader import YDbfReader
from ydbf.writer import YDbfWriter
