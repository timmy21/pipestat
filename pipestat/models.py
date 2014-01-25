# -*- coding: utf-8 -*-

import copy


class Document(dict):

    def get(self, key, default=None):
        parts = key.split(".")
        doc = self
        try:
            for part in parts:
                if part in doc:
                    doc = doc[part]
                else:
                    return default
        except:
            raise KeyError('Invalid document key "%s"' % key)
        return copy.deepcopy(doc)

    def set(self, key, value):
        parts = key.split(".")
        doc = self
        for part in parts[:-1]:
            if part not in doc:
                doc[part] = {}
            doc = doc[part]
        doc[parts[-1]] = copy.deepcopy(value)
