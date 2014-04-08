# -*- coding: utf-8 -*-
import types
from datetime import datetime, date

ASCENDING  = 1
DESCENDING = -1

NumberTypes = (types.IntType, types.LongType, types.FloatType)
DateTypes = tuple(list(NumberTypes)+[date, datetime])

ArrayTypes = (list, tuple, set)
