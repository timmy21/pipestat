# -*- coding: utf-8 -*-
import types
from datetime import datetime, date

ASCENDING  = 1
DESCENDING = -1

NumberTypes = (types.IntType, types.LongType, types.FloatType)
DateTypes = tuple(list(NumberTypes)+[date, datetime])

ArrayTypes = (list, tuple, set)


# use for solve performance issue
VALUE_TYPE_PLAIN = 0
VALUE_TYPE_REFKEY = 1
VALUE_TYPE_OPERATOR = 2
