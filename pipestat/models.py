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
    so juge if comma in key before call function, that is ugly but faster.

    >>> doc = Document({'flag': {'fin': 1}})
    >>> val = doc.get('flag.fin')
    """

    def get(self, key, default=None):
        try:
            parts = key.split(".")
            pcnt = len(parts)
            doc = self
            for i, part in enumerate(parts):
                doc = doc[part]
                if i != pcnt - 1 and isinstance(doc, ArrayTypes):
                    remain_parts = ".".join(parts[i+1:])
                    if remain_parts:
                        doc = (Document(x).get(remain_parts, undefined) for x in doc)
                        doc = [x for x in doc if x != undefined]
                    break
            return doc
        except Exception:
            return default

    def set(self, key, value):
        parts = key.split(".")
        doc = self
        for part in parts[:-1]:
            if part not in doc:
                doc[part] = {}
            doc = doc[part]
        doc[parts[-1]] = value

    def setdefault(self, key, value):
        val = self.get(key, undefined)
        if val == undefined:
            self.set(key, value)
            return value
        else:
            return val

    def has(self, key):
        val = self.get(key, undefined)
        return val != undefined

    #only support top level
    def pop(self, key, *args, **kwargs):
        if "default" in kwargs:
            return dict.pop(self, key, kwargs["default"])
        elif len(args):
            return dict.pop(self, key, args[0])
        else:
            return dict.pop(self, key)

    def delete(self, key):
        try:
            parts = key.split(".")
            doc = self
            for part in parts[:-1]:
                doc = doc[part]
            del doc[parts[-1]]
        except Exception:
            pass
