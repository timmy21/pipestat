# -*- coding: utf-8 -*-
from pipestat.constants import NumberTypes, DateTypes


class Value(object):

    @staticmethod
    def is_doc_ref_key(val):
        try:
            if val[0] == "$":
                return True
            else:
                return False
        except Exception:
            return False

    @staticmethod
    def is_operator(val):
        try:
            if val[0] == "$":
                return True
            else:
                return False
        except Exception:
            return False


def isNumberType(val):
    return isinstance(val, NumberTypes)

def isDateType(val):
    return isinstance(val, DateTypes)
