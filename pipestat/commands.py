# -*- coding: utf-8 -*-

import json
import copy
import collections
from pipestat.bsort import insort
from pipestat.errors import PipelineError, CommandError, LimitCompleted
from pipestat.operator import OperatorFactory, ProjectOperator
from pipestat.models import Document
from pipestat.utils import Value
from pipestat.constants import ASCENDING, DESCENDING


_commands = {}


class CommandFactory(object):

    @staticmethod
    def new(value):
        def is_valid():
            if not isinstance(value, dict):
                return False
            if len(value) != 1:
                return False
            cmd_k = value.keys()[0]
            if not cmd_k:
                return False
            if cmd_k[0] != "$":
                return False
            return True

        if not is_valid():
            raise PipelineError("invalid command '%s'" % value)
        cmd_k = value.keys()[0]
        cmd_v = value[cmd_k]
        if cmd_k in _commands:
            return _commands[cmd_k](cmd_v)
        else:
            raise PipelineError("invalid command '%s'" % value)


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
            raise self.make_error("the $match command requires dict type")
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
        if not isinstance(value, dict) or not value:
            raise self.make_error("the $project command requires non-empty dict type")
        operators = {}
        excludes = set()
        for k, v in value.iteritems():
            if v == 0:
                excludes.add(k)
            else:
                operators[k] = OperatorFactory.new_project(k, v)
        if operators and excludes:
            raise self.make_error("the $project command cannot mix use exclusion and inclusion")
        self.operators = operators
        self.excludes = excludes

    def feed(self, document):
        if self.operators:
            new_doc = Document()
            for k, op in self.operators.iteritems():
                new_doc.set(k, op.project(document))
        else:
            new_doc = Document(copy.deepcopy(document))
            for k in self.excludes:
                new_doc.delete(k)
        super(ProjectCommand, self).feed(new_doc)


class GroupCommand(Command):

    name = "$group"

    def __init__(self, value):
        super(GroupCommand, self).__init__(value)
        if not isinstance(value, dict):
            raise self.make_error("the $group command requires dict type")
        elif "_id" not in value:
            raise self.make_error("the $group command requires '_id' field")
        operators = {}
        for k, v in value.iteritems():
            if k == "_id":
                continue
            operators[k] = OperatorFactory.new_group(k, v)
        self.operators = operators
        self._id = self._valid_id(value["_id"])
        self._id_docs = {}

    def feed(self, document):
        ids = self.gen_id(document)
        gid = json.dumps({"_id": ids})
        acc_vals = self._id_docs.setdefault(gid, Document())
        for k, op in self.operators.iteritems():
            acc_vals.set(k, op.group(document, acc_vals.get(k)))

    def result(self):
        documents = self._make_result()
        if self.next:
            try:
                for doc in documents:
                    self.next.feed(doc)
            except LimitCompleted:
                pass
            return self.next.result()
        else:
            return documents

    def _make_result(self):
        rets = []
        for k, v in self._id_docs.iteritems():
            k = json.loads(k)
            rets.append(Document(dict(k, **v)))
        return rets

    def _valid_id(self, id_v):
        if isinstance(id_v, dict):
            return OperatorFactory.new_expression("_id", id_v)
        else:
            return id_v

    def gen_id(self, document):
        if isinstance(self._id, ProjectOperator):
            return self._id.project(document)
        elif Value.is_doc_ref_key(self._id):
            return document.get(self._id[1:])
        else:
            return self._id


class SortCommand(Command):

    name = "$sort"

    def __init__(self, value):
        super(SortCommand, self).__init__(value)
        if isinstance(value, (list, tuple)):
            for k, direction in value:
                if not isinstance(k, basestring):
                    raise self.make_error("$sort: key must be field name")
                if (direction not in [ASCENDING, DESCENDING]):
                    raise self.make_error("$sort: direction must be 1 or -1")
        elif isinstance(value, (dict, collections.OrderedDict)):
            for k, direction in value.iteritems():
                if not isinstance(k, basestring):
                    raise self.make_error("$sort: key must be field name")
                if (direction not in [ASCENDING, DESCENDING]):
                    raise self.make_error("$sort: direction must be 1 or -1")

                self.value = [(k, direction) for k, direction in value.iteritems()]
        else:
            raise self.make_error("the $sort command requires a list of (key, direction) pairs or a dict of {key: direction}")

    def feed(self, document):
        insort(self.documents, document, cmp=self.cmp_func)

    def cmp_func(self, doc1, doc2):
        for k, direction in self.value:
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
        try:
            self.value = int(value)
        except Exception:
            raise self.make_error("the $skip command requires numeric type")
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
        try:
            self.value = int(value)
        except Exception:
            raise self.make_error("the $limit command requires numeric type")
        self._received = 0

    def feed(self, document):
        if self._received < self.value:
            self._received += 1
            super(LimitCommand, self).feed(document)
        else:
            raise LimitCompleted('the $limit command alreay received %d documents' % self.value)


class UnwindCommand(Command):

    name = "$unwind"

    def __init__(self, value):
        super(UnwindCommand, self).__init__(value)
        if not Value.is_doc_ref_key(value):
            raise self.make_error("the $unwind command requires ref-key")

    def feed(self, document):
        vals = document.get(self.value[1:])
        for v in vals:
            new_doc = Document(copy.deepcopy(document))
            new_doc.set(self.value[1:], v)
            super(UnwindCommand, self).feed(new_doc)
