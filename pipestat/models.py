# -*- coding: utf-8 -*-

import copy
from pipestat.constants import ArrayTypes


class _Undefined(object):

    def __eq__(self, other):
        if isinstance(other, _Undefined):
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return False

    def __le__(self, other):
        if isinstance(other, _Undefined):
            return True
        return False

    def __gt__(self, other):
        return False

    def __ge__(self, other):
        if isinstance(other, _Undefined):
            return True
        return False

    def __nonzero__(self):
        return False

undefined = _Undefined()


class Document(dict):

    def get(self, key, default=None):
        parts = key.split(".")
        doc = self
        try:
            for i, part in enumerate(parts):
                doc = doc[part]
                if isinstance(doc, ArrayTypes):
                    remain_parts = ".".join(parts[i+1:])
                    if remain_parts:
                        doc = map(lambda x: Document(x).get(remain_parts, undefined), doc)
                        doc = filter(lambda x: x != undefined, doc)
                    break
        except Exception:
            return default
        return copy.deepcopy(doc)

    def set(self, key, value):
        parts = key.split(".")
        doc = self
        for part in parts[:-1]:
            if part not in doc:
                doc[part] = {}
            doc = doc[part]
        doc[parts[-1]] = copy.deepcopy(value)

    def delete(self, key):
        parts = key.split(".")
        doc = self
        try:
            for part in parts[:-1]:
                doc = doc[part]
            del doc[parts[-1]]
        except Exception:
            pass
