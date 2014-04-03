# -*- coding: utf-8 -*-
import types


class Value(object):

    @staticmethod
    def is_doc_ref_key(val):
        if not isinstance(val, basestring):
            return False
        if len(val) == 0:
            return False
        if val[0] != "$":
            return False
        return True

    @staticmethod
    def is_operator(val):
        if not isinstance(val, basestring):
            return False
        if len(val) == 0:
            return False
        if val[0] != "$":
            return False
        return True


def isNumberType(val):
    NumberTypes = (types.IntType, types.LongType, types.FloatType)
    return isinstance(val, NumberTypes)
