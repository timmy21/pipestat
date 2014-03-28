# -*- coding: utf-8 -*-

import copy


class Document(dict):

    def get(self, key, default=None):
        parts = key.split(".")
        doc = self
        try:
            for part in parts:
                doc = doc[part]
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
