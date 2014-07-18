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

    def get(self, key, default=None):
        try:
            if "." not in key:
                doc = self[key]
            else:
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
        except Exception:
            doc = default
        return doc

    def set(self, key, value):
        if "." not in key:
            self[key] = value
        else:
            parts = key.split(".")
            doc = self
            for part in parts[:-1]:
                if part not in doc:
                    doc[part] = {}
                doc = doc[part]
            doc[parts[-1]] = value

    def delete(self, key):
        try:
            if '.' not in key:
                del self[key]
            else:
                parts = key.split(".")
                doc = self
                for part in parts[:-1]:
                    doc = doc[part]
                del doc[parts[-1]]
        except Exception:
            pass
