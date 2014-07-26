# -*- coding: utf-8 -*-

try:
    import simplejson as json
except ImportError:
    import json
import collections
import itertools
from pipestat.bsort import insort
from pipestat.errors import PipelineError, CommandError, LimitCompleted
from pipestat.operator import OperatorFactory, ProjectOperator
from pipestat.models import Document, undefined
from pipestat.utils import Value, isNumberType
from pipestat.constants import ASCENDING, DESCENDING, ArrayTypes
from pipestat.constants import VALUE_TYPE_PLAIN, VALUE_TYPE_REFKEY, VALUE_TYPE_OPERATOR


_commands = {}


class CommandFactory(object):

    @staticmethod
    def new(value):
        if not isinstance(value, dict):
            raise PipelineError("pipeline element is not an object")
        if len(value) != 1:
            raise PipelineError("pipeline specification object must contain exactly one field")
        cmd_k = value.keys()[0]
        cmd_v = value[cmd_k]
        if cmd_k in _commands:
            return _commands[cmd_k](cmd_v)
        else:
            raise PipelineError("unknow pipeline command '%s'" % cmd_k)


class CommandMeta(type):

    def __init__(cls, name, bases, attrs):
        command = getattr(cls, "name", None)
        if command and command not in _commands:
            _commands[command] = cls
        super(CommandMeta, cls).__init__(name, bases, attrs)


class Command(object):

    __metaclass__ = CommandMeta

    def __init__(self, value):
        self.value = value
        self.next = None
        self.documents = []

    def feed(self, document):
        if self.next:
            self.next.feed(document)
        else:
            self.documents.append(document)

    def result(self):
        if self.next:
            return self.next.result()
        else:
            return self.documents

    def make_error(self, message):
        return CommandError(message, self.name)


class MatchCommand(Command):

    name = "$match"

    def __init__(self, value):
        super(MatchCommand, self).__init__(value)
        if not isinstance(value, dict):
            raise self.make_error("$match specification must be an object")
        operators = []
        for k, v in value.iteritems():
            operators.append(OperatorFactory.new_match(k, v))
        self.operators = operators

    def feed(self, document):
        matched = True
        for op in self.operators:
            if not op.match(document):
                matched = False
                break

        if matched:
            super(MatchCommand, self).feed(document)


class ProjectCommand(Command):

    name = "$project"

    def __init__(self, value):
        super(ProjectCommand, self).__init__(value)
        if not isinstance(value, dict):
            raise self.make_error("$project specification must be an object")
        if not value:
            raise self.make_error("$projec requires at least one output field")
        plain_operators = []
        comma_operators = []
        excludes = set()
        for k, v in value.iteritems():
            if v == 0:
                excludes.add(k)
            else:
                if "." in k:
                    comma_operators.append((k, OperatorFactory.new_project(k, v)))
                else:
                    plain_operators.append((k, OperatorFactory.new_project(k, v)))
        if (plain_operators or comma_operators) and excludes:
            raise self.make_error("$project cannot mix use exclusion and inclusion")

        self.plain_operators = plain_operators
        self.comma_operators = comma_operators
        self.excludes = excludes

        if self.excludes:
            self.feed = self.feed_excludes
        else:
            self.feed = self.feed_operators

    def feed_operators(self, document):
        new_doc = Document()
        for k, op in self.plain_operators:
            v = op.project(document)
            if v != undefined:
                new_doc[k] = v

        for k, op in self.comma_operators:
            v = op.project(document)
            if v != undefined:
                new_doc.set2(k, v)

        super(ProjectCommand, self).feed(new_doc)

    def feed_excludes(self, document):
        new_doc = Document()
        for k, v in document.iteritems():
            if k not in self.excludes:
                if "." in k:
                    new_doc.set2(k, v)
                else:
                    new_doc[k] = v
        super(ProjectCommand, self).feed(new_doc)


class GroupCommand(Command):

    name = "$group"

    def __init__(self, value):
        super(GroupCommand, self).__init__(value)
        if not isinstance(value, dict):
            raise self.make_error("$group specification must be an object")
        elif "_id" not in value:
            raise self.make_error("$group specification must include an _id")
        plain_operators = []
        comma_operators = []
        for k, v in value.iteritems():
            if k == "_id":
                continue
            if "." in k:
                comma_operators.append((k, OperatorFactory.new_group(k, v)))
            else:
                plain_operators.append((k, OperatorFactory.new_group(k, v)))
        self.plain_operators = plain_operators
        self.comma_operators = comma_operators

        id_v = value["_id"]
        if Value.is_doc_ref_key(id_v):
            self._id = id_v[1:]
            self._id_type = VALUE_TYPE_REFKEY
            self._id_comma = "." in self._id
        elif isinstance(id_v, dict):
            self._id = OperatorFactory.new_project("_id", id_v)
            self._id_type = VALUE_TYPE_OPERATOR
        else:
            self._id = id_v
            self._id_type = VALUE_TYPE_PLAIN

        self._id_docs = {}

    def init_doc(self, ids):
        doc = Document(_id=ids)
        for k, op in self.plain_operators:
            doc[k] = op.init_val()
        for k, op in self.comma_operators:
            doc.set2(k, op.init_val())
        return doc

    def feed(self, document):
        ids = self.gen_id(document)
        hid = self.hash_id(ids)
        if hid in self._id_docs:
            acc_doc = self._id_docs[hid]
        else:
            acc_doc = self._id_docs[hid] = self.init_doc(ids)
        for k, op in self.plain_operators:
            acc_doc[k] = op.group(document, acc_doc[k])

        for k, op in self.comma_operators:
            acc_doc.set2(k, op.group(document, acc_doc.get2(k)))

    def result(self):
        self.normalize()

        if self.next:
            try:
                for doc in self._id_docs.itervalues():
                    self.next.feed(doc)
            except LimitCompleted:
                pass
            return self.next.result()
        else:
            return self._id_docs.values()

    def normalize(self):
        for acc_doc in self._id_docs.itervalues():
            for k, op in self.plain_operators:
                acc_doc[k] = op.result(acc_doc.get(k))

            for k, op in self.comma_operators:
                acc_doc.set2(k, op.result(acc_doc.get2(k)))

    def gen_id(self, document):
        if self._id_type == VALUE_TYPE_REFKEY:
            if self._id_comma:
                return document.get2(self._id)
            else:
                return document.get(self._id)
        elif self._id_type == VALUE_TYPE_OPERATOR:
            return self._id.project(document)
        else:
            return self._id

    def hash_id(self, ids):
        try:
            if isinstance(ids, dict):
                return hash(frozenset(ids.items()))
            else:
                return hash(ids)
        except Exception:
            return hash(json.dumps({"_id": ids}))


class SortCommand(Command):

    name = "$sort"

    def __init__(self, value):
        super(SortCommand, self).__init__(value)
        if isinstance(value, (list, tuple)):
            for k, direction in value:
                if not isinstance(k, basestring):
                    raise self.make_error("$sort field must be string type")
                if (direction not in [ASCENDING, DESCENDING]):
                    raise self.make_error("$sort direction must be 1 or -1")
        elif isinstance(value, (dict, collections.OrderedDict)):
            for k, direction in value.iteritems():
                if not isinstance(k, basestring):
                    raise self.make_error("$sort field must be string type")
                if (direction not in [ASCENDING, DESCENDING]):
                    raise self.make_error("$sort direction must be 1 or -1")

                self.value = [(k, direction) for k, direction in value.iteritems()]
        else:
            raise self.make_error("$sort specification must be a list or a object")

    def feed(self, document):
        insort(self.documents, document, cmp=self.cmp_func)

    def cmp_func(self, doc1, doc2):
        for k, direction in self.value:
            if "." in k:
                v1 = doc1.get2(k)
                v2 = doc2.get2(k)
            else:
                v1 = doc1.get(k)
                v2 = doc2.get(k)
            ret = 0
            if direction == ASCENDING:
                ret = cmp(v1, v2)
            else:
                ret = cmp(v2, v1)
            if ret == 0:
                continue
            else:
                return ret
        return 0

    def result(self):
        if self.next:
            try:
                for doc in self.documents:
                    self.next.feed(doc)
            except LimitCompleted:
                pass
            return self.next.result()
        else:
            return self.documents


class SkipCommand(Command):

    name = "$skip"

    def __init__(self, value):
        super(SkipCommand, self).__init__(value)
        if isNumberType(value):
            self.value = int(value)
        else:
            raise self.make_error("$skip specification must be numeric type")
        self._skiped = 0

    def feed(self, document):
        if self._skiped >= self.value:
            super(SkipCommand, self).feed(document)
        else:
            self._skiped += 1


class LimitCommand(Command):

    name = "$limit"

    def __init__(self, value):
        super(LimitCommand, self).__init__(value)
        if isNumberType(value):
            self.value = int(value)
        else:
            raise self.make_error("$limit specification must be numeric type")
        self._received = 0

    def feed(self, document):
        if self._received < self.value:
            self._received += 1
            super(LimitCommand, self).feed(document)
        else:
            raise LimitCompleted('$limit alreay received %d documents' % self.value)


class UnwindCommand(Command):

    name = "$unwind"

    def __init__(self, value):
        super(UnwindCommand, self).__init__(value)
        if not Value.is_doc_ref_key(value):
            raise self.make_error("$unwind field path references must be prefixed with a '$'")
        self.value = value[1:]
        self.value_comma = "." in self.value

    def feed(self, document):
        if self.value_comma:
            vals = document.get2(self.value, undefined)
        else:
            vals = document.get(self.value, undefined)
        if vals != undefined:
            if not isinstance(vals, ArrayTypes):
                raise self.make_error("$unwind value at end of field path must be an array")

            for v in vals:
                new_doc = Document(document)
                if self.value_comma:
                    new_doc.set2(self.value, v)
                else:
                    new_doc[self.value] = v
                super(UnwindCommand, self).feed(new_doc)
