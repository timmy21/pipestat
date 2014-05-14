# -*- coding: utf-8 -*-
import json
from pipestat.contrib import xmltodict
from pipestat.errors import PipeStatParseError


def xsplit(v, separator, count=-1):
    return filter(None, map(lambda x: x.strip(), v.split(separator, count)))


class Parser(object):

    @classmethod
    def parse(cls, name, v, *args, **kwargs):
        if hasattr(cls, name) and name not in ["parse", "get"]:
            return getattr(cls, name)(v, *args, **kwargs)
        raise PipeStatParseError("Cannot find parser:%s" % name)

    @classmethod
    def get(cls, name):
        if hasattr(cls, name):
            return getattr(cls, name)
        raise PipeStatParseError("Cannot find parser:%s" % name)

    @classmethod
    def int(cls, v):
        try:
            return int(v)
        except Exception:
            raise PipeStatParseError("cannot parse value:%r with int parser" % v)

    @classmethod
    def float(cls, v):
        try:
            return float(v)
        except Exception:
            raise PipeStatParseError("cannot parse value:%r with float parser" % v)

    @classmethod
    def json(cls, v):
        try:
            return json.loads(v)
        except Exception:
            raise PipeStatParseError("cannot parse value:%r with json parser" % v)

    @classmethod
    def xml(cls, v):
        try:
            return xmltodict.parse(v, dict_constructor=dict)
        except Exception:
            raise PipeStatParseError("cannot parse value:%r with xml parser" % v)

    @classmethod
    def keyvalue(cls, v, equal="=", multi=","):
        try:
            retv = {}
            parts = xsplit(v, ",")
            for part in parts:
                k, v = xsplit(part, "=", 1)
                retv[k] = v
            return retv
        except Exception:
            raise PipeStatParseError("cannot parse value:%r with keyvalue parser" % v)

    @classmethod
    def items(cls, v, multi=","):
        try:
            return xsplit(v, multi)
        except Exception:
            raise PipeStatParseError("cannot parse value:%r with items parser" % v)
