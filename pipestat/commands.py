# -*- coding: utf-8 -*-

import json
import copy
import collections
from pipestat.bsort import insort
from pipestat.errors import CommandError, LimitCompleted
from pipestat.operator import OperatorFactory
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
            raise CommandError("invalid command %r" % value)
        cmd_k = value.keys()[0]
        cmd_v = value[cmd_k]
        if cmd_k in _commands:
            return _commands[cmd_k](cmd_v)
        else:
            raise CommandError("invalid command %r" % value)


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
        return CommandError('invalid command %s value=%r message="%s"' % (
            self.name, self.value, message
        ))


class MatchCommand(Command):

    name = "$match"

    def __init__(self, value):
        super(MatchCommand, self).__init__(value)
        if not isinstance(value, dict):
            raise self.make_error("value is invalid")
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
            raise self.make_error("value is invalid")
        operators = []
        for k, v in value.iteritems():
            operators.append(OperatorFactory.new_project(k, v))
        self.operators = operators

    def feed(self, document):
        new_doc = Document()
        for op in self.operators:
            new_doc.update(op.project(document))
        super(ProjectCommand, self).feed(new_doc)


class GroupCommand(Command):

    name = "$group"

    def __init__(self, value):
        super(GroupCommand, self).__init__(value)
        if not isinstance(value, dict):
            raise self.make_error("value is invalid")
        elif "_id" not in value:
            raise self.make_error('value not have "_id" field')
        operators = {}
        for k, v in value.iteritems():
            if k == "_id":
                continue
            operators[k] = OperatorFactory.new_group(k, v)
        self.operators = operators
        self._id_docs = {}

    def feed(self, document):
        ids = self.gen_id(document, self.value["_id"])
        gid = json.dumps({"_id": ids})
        acc_vals = self._id_docs.setdefault(gid, {})
        for k, op in self.operators.iteritems():
            acc_vals[k] = op.group(document, acc_vals.get(k))

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

    def gen_id(self, document, id_v):
        if Value.is_doc_ref_key(id_v):
            return document.get(id_v[1:])
        elif isinstance(id_v, dict):
            ids = {}
            for k, v in id_v.iteritems():
                ids[k] = self.gen_id(document, v)
            return ids
        elif isinstance(id_v, collections.Iterable):
            ids = []
            for v in id_v:
                ids.append(self.gen_id(document, v))
            return ids
        else:
            return id_v


class SortCommand(Command):

    name = "$sort"

    def __init__(self, value):
        super(SortCommand, self).__init__(value)
        if not isinstance(value, collections.Iterable):
            raise self.make_error("value is not iterable")
        try:
            for k, direction in value:
                if not isinstance(k, basestring):
                    raise ValueError("invalid sort key %r" % k)
                if (direction not in [ASCENDING, DESCENDING]):
                    raise ValueError("invalid sort direction %r" % direction)
        except Exception, e:
            raise self.make_error(str(e))

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
        except Exception, e:
            raise self.make_error(str(e))
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
        except Exception, e:
            raise self.make_error(str(e))
        self._received = 0

    def feed(self, document):
        if self._received < self.value:
            self._received += 1
            super(LimitCommand, self).feed(document)
        else:
            raise LimitCompleted('command $limit alreay received %d documents' % self.value)


class UnwindCommand(Command):

    name = "$unwind"

    def __init__(self, value):
        super(UnwindCommand, self).__init__(value)
        if not Value.is_doc_ref_key(value):
            raise self.make_error("value is not document ref-key")

    def feed(self, document):
        vals = document.get(self.value[1:])
        for v in vals:
            new_doc = Document(copy.deepcopy(document))
            new_doc.set(self.value[1:], v)
            super(UnwindCommand, self).feed(new_doc)
