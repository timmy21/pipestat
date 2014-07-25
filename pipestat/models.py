# -*- coding: utf-8 -*-

from pipestat.constants import ArrayTypes


class _Undefined(object):

    def __lt__(self, other):
        return False

    def __le__(self, other):
        return self == other

    def __gt__(self, other):
        return False

    def __ge__(self, other):
        return self == other

    def __nonzero__(self):
        return False

undefined = _Undefined()


class Document(dict):
    """Dict Wrapper for nested key, for example: 'flag.fin'

    v2 function is more slower than same function in dict,
    so juge if comma in key before call relative function, for example below:

    >>> doc = Document({'flag': {'fin': 1}})
    >>> key = 'flag.fin'
    >>> if '.' in key:
    ...     val = doc.get2(key)
    ... else:
    ...     val = doc.get(key)
    """

    def get2(self, key, default=None):
        try:
            parts = key.split(".")
            pcnt = len(parts)
            doc = self
            for i, part in enumerate(parts):
                doc = doc[part]
                if i != pcnt - 1 and isinstance(doc, ArrayTypes):
                    remain_parts = ".".join(parts[i+1:])
                    if remain_parts:
                        doc = (Document(x).get2(remain_parts, undefined) for x in doc)
                        doc = [x for x in doc if x != undefined]
                    break
            return doc
        except Exception:
            return default

    def set2(self, key, value):
        parts = key.split(".")
        doc = self
        for part in parts[:-1]:
            if part not in doc:
                doc[part] = {}
            doc = doc[part]
        doc[parts[-1]] = value


    def setdefault2(self, key, value):
        val = self.get2(key, undefined)
        if val == undefined:
            self.set2(key, value)
            return value
        else:
            return val

    def has2(self, key):
        val = self.get2(key, undefined)
        return val != undefined

    def pop2(self, key, **kwargs):
        val = self.get2(key, undefined)
        if val == undefined:
            if "default" in kwargs:
                return kwargs["default"]
            else:
                raise KeyError(key)
        else:
            return val

    def delete2(self, key):
        try:
            parts = key.split(".")
            doc = self
            for part in parts[:-1]:
                doc = doc[part]
            del doc[parts[-1]]
        except Exception:
            pass
