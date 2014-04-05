# -*- coding: utf-8 -*-

import re
import time
import string
import collections
from pipestat.errors import OperatorError, CommandError
from pipestat.utils import Value, isNumberType
from pipestat.models import Document, undefined
from datetime import datetime, date


_operators = {}


class OperatorFactory(object):

    @staticmethod
    def new_match(key, value):
        if key == "$and":
            return MatchAndOperator(value)
        elif key == "$or":
            return MatchOrOperator(value)
        elif key == "$call":
            return MatchCallOperator(value)

        if not isinstance(value, dict):
            return MatchEqualOperator(key, value)
        else:
            if len(value) == 1:
                name = value.keys()[0]
                match_operators = _operators.get("$match", {})
                if name in ["$and", "$or", "$call"]:
                    raise CommandError("invalid operator '%s'" % name)
                if name in match_operators:
                    return match_operators[name](key, value[name])
                else:
                    raise CommandError("unknow $match operator '%s'" % name, "$match")
            else:
                return MatchCombineOperator(key, value)

    @staticmethod
    def new_project(key, value):
        if not isinstance(value, dict):
            return ProjectValueOperator(key, value)
        else:
            if len(value) == 1:
                name = value.keys()[0]
                project_operators = _operators.get("$project", {})
                if Value.is_operator(name):
                    if name in project_operators:
                        return project_operators[name](key, value[name])
                    else:
                        raise CommandError("unknow $project operator '%s'" % name, "$project")
            return ProjectCombineOperator(key, value)

    @staticmethod
    def new_group(key, value):
        if not isinstance(value, dict):
            raise CommandError("the $group aggregate field '%s' must be defined as an expression inside an object" % key)

        if len(value) == 1:
            name = value.keys()[0]
            group_operators = _operators.get("$group", {})
            if Value.is_operator(name):
                if name in group_operators:
                    return group_operators[name](key, value[name])
                else:
                    raise CommandError("unknow $group operator '%s'" % name, "$group")
        if value:
            return GroupCombineOperator(key, value)
        else:
            raise CommandError("the computed aggregate '%s' must specify exactly one operator" % key)


class OperatorMeta(type):

    def __init__(cls, name, bases, attrs):
        command = getattr(cls, "command", None)
        operator = getattr(cls, "name", None)
        if command and operator:
            if command not in _operators:
                _operators[command] = {}
            _operators[command][operator] = cls
        super(OperatorMeta, cls).__init__(name, bases, attrs)


class Operator(object):

    __metaclass__ = OperatorMeta

    def make_error(self, message):
        return OperatorError(message, self.command, self.name)


class MatchOperator(Operator):

    command = "$match"

    def match(self, document):
        try:
            return self.eval(document)
        except OperatorError:
            raise
        except Exception, e:
            raise self.make_error("%s runtime error: %s" % (self.name, str(e)))

    def eval(self, document):
        raise NotImplemented()


class MatchKeyOperator(MatchOperator):

    def __init__(self, key, value):
        self.key = key
        self.value = value


class MatchRegexpOperator(MatchKeyOperator):

    name = "$regexp"

    def __init__(self, key, value):
        super(MatchRegexpOperator, self).__init__(key, value)
        try:
            self.pat = re.compile(value)
        except Exception:
            raise self.make_error("the $regexp operator requires regular expression")

    def eval(self, document):
        doc_val = document.get(self.key)
        if not isinstance(doc_val, basestring):
            return False
        m = self.pat.search(doc_val)
        if m:
            return True
        return False


class MatchCmpOperator(MatchKeyOperator):

    def eval(self, document):
        doc_val = document.get(self.key, undefined)
        value = self.value
        if Value.is_doc_ref_key(value):
            value = document.get(value[1:], undefined)
        return self.cmp(doc_val, value)

    def cmp(self, doc_val, value):
        raise NotImplementedError()


class MatchLTOperator(MatchCmpOperator):

    name = "$lt"

    def cmp(self, doc_val, value):
        return doc_val < value


class MatchLTEOperator(MatchCmpOperator):

    name = "$lte"

    def cmp(self, doc_val, value):
        return doc_val <= value


class MatchGTOperator(MatchCmpOperator):

    name = "$gt"

    def cmp(self, doc_val, value):
        return doc_val > value


class MatchGTEOperator(MatchCmpOperator):

    name = "$gte"

    def cmp(self, doc_val, value):
        return doc_val >= value


class MatchEqualOperator(MatchCmpOperator):

    name = "$eq"

    def cmp(self, doc_val, value):
        return doc_val == value


class MatchNotEqualOperator(MatchCmpOperator):

    name = "$ne"

    def cmp(self, doc_val, value):
        return doc_val != value


class MatchBelongOperator(MatchKeyOperator):

    def __init__(self, key, value):
        super(MatchBelongOperator, self).__init__(key, value)
        if isinstance(value, collections.Iterable):
            super(MatchBelongOperator, self).__init__(key, value)
        else:
            raise self.make_error("the %s operator requires iterable" % self.name)

    def eval(self, document):
        doc_val = document.get(self.key, undefined)
        return self.belong(doc_val, self.value)

    def belong(self, doc_val, value):
        raise NotImplementedError()


class MatchInOperator(MatchBelongOperator):

    name = "$in"

    def belong(self, doc_val, value):
        return doc_val in value


class MatchNotInOperator(MatchBelongOperator):

    name = "$nin"

    def belong(self, doc_val, value):
        return doc_val not in value


class MatchCallOperator(MatchOperator):

    name = "$call"

    def __init__(self, value):
        self.value = value
        if not callable(value):
            raise self.make_error("the $call operator requires callable")

    def eval(self, document):
        if self.value(document):
            return True
        return False


class LogicSubOperator(object):

    def __init__(self):
        self.operators = []

    def add(self, op):
        self.operators.append(op)

    def match(self, document):
        for op in self.operators:
            if not op.match(document):
                return False
        return True


class MatchLogicOperator(MatchOperator):

    def __init__(self, value):
        self.value = value
        if isinstance(value, list):
            sub_ops = []
            for sub_op in value:
                if isinstance(sub_op, dict):
                    sop = LogicSubOperator()
                    for k, v in sub_op.iteritems():
                        sop.add(OperatorFactory.new_match(k, v))
                    sub_ops.append(sop)
                else:
                    raise self.make_error('%s match element must be object' % self.name)
            self.sub_ops = sub_ops
        else:
            raise self.make_error('the %s operator requires a nonempty list' % self.name)


class MatchAndOperator(MatchLogicOperator):

    name = "$and"

    def eval(self, document):
        for sub_op in self.sub_ops:
            if not sub_op.match(document):
                return False
        return True


class MatchOrOperator(MatchLogicOperator):

    name = "$or"

    def eval(self, document):
        for sub_op in self.sub_ops:
            if sub_op.match(document):
                return True
        return False


class MatchCombineOperator(MatchKeyOperator):

    def __init__(self, key, value):
        super(MatchCombineOperator, self).__init__(key, value)
        combined_ops = []
        for k, v in value.iteritems():
            combined_ops.append(OperatorFactory.new_match(key, {k: v}))
        self.combined_ops = combined_ops

    def eval(self, document):
        for combine_op in self.combined_ops:
            if not combine_op.match(document):
                return False
        return True


class ProjectOperator(Operator):

    command = "$project"

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def project(self, document):
        try:
            return self.eval(document)
        except OperatorError:
            raise
        except Exception, e:
            raise self.make_error("%s runtime error: %s" % (self.name, str(e)))

    def eval(self, document):
        raise NotImplemented()


class ProjectValueOperator(ProjectOperator):

    name = "$value"

    def __init__(self, key, value):
        super(ProjectValueOperator, self).__init__(key, value)
        if not (Value.is_doc_ref_key(value) or value == 1):
            raise self.make_error("field path references must be prefixed with a '$'")

    def eval(self, document):
        if Value.is_doc_ref_key(self.value):
            return document.get(self.value[1:], undefined)
        elif self.value == 1:
            return document.get(self.key, undefined)


class ProjectExtractOperator(ProjectOperator):

    name = "$extract"

    def __init__(self, key, value):
        super(ProjectExtractOperator, self).__init__(key, value)
        if isinstance(value, list) and len(value) == 2:
            if not Value.is_doc_ref_key(value[0]):
                if isinstance(value[0], dict):
                    if len(value[0]) == 1:
                        self.value[0] = OperatorFactory.new_project(key, value[0])
                    else:
                        raise self.make_error("$extract parameter operator must contain exactly one field")
                else:
                    raise self.make_error("$extract field path references must be prefixed with a '$'")
            try:
                self.value[1] = re.compile(value[1])
            except Exception:
                raise self.make_error("$extract pattern must be regular expression")
        else:
            raise self.make_error("the $extract operator requires an array of two elements")

    def eval(self, document):
        v = self.value[0]
        if isinstance(v, ProjectOperator):
            v = v.eval(document)
        elif Value.is_doc_ref_key(v):
            v = document.get(v[1:], undefined)

        if not isinstance(v, basestring):
            raise self.make_error("$extract source must be string type")
        else:
            m = self.value[1].search(v)
            if m:
                if self.key in m.groupdict():
                    return m.groupdict()[self.key]
                elif len(m.groups()) > 0:
                    return m.group(1)
                else:
                    return m.group()


class ProjectTimestampOperator(ProjectOperator):

    name = "$timestamp"

    def __init__(self, key, value):
        super(ProjectTimestampOperator, self).__init__(key, value)
        if isinstance(value, list) and len(value) == 2:
            if not Value.is_doc_ref_key(value[0]):
                if isinstance(value[0], dict):
                    if len(value[0]) == 1:
                        self.value[0] = OperatorFactory.new_project(key, value[0])
                    else:
                        raise self.make_error("$timestamp parameter operator must contain exactly one field")
                else:
                    raise self.make_error("$timestamp field path references must be prefixed with a '$'")

            if not isinstance(value[1], basestring):
                raise self.make_error("$timestamp format must be string type")
        else:
            raise self.make_error("the $timestamp operator requires an array of two elements")

    def eval(self, document):
        v = self.value[0]
        if isinstance(v, ProjectOperator):
            v = v.eval(document)
        elif Value.is_doc_ref_key(v):
            v = document.get(v[1:])

        if not isinstance(v, basestring):
            raise self.make_error("$timestamp source must be string type")
        else:
            return time.mktime(time.strptime(v, self.value[1]))


class ProjectDualNumberOperator(ProjectOperator):

    def __init__(self, key, value):
        super(ProjectDualNumberOperator, self).__init__(key, value)
        if isinstance(value, list) and len(value) == 2:
            for i, v in enumerate(value):
                if not Value.is_doc_ref_key(v):
                    if isinstance(v, dict):
                        if len(v) == 1:
                            self.value[i] = OperatorFactory.new_project(key, v)
                        else:
                            raise self.make_error("%s parameter operator must contain exactly one field" % self.name)
                    elif isNumberType(v):
                        self.value[i] = float(v)
                    else:
                        raise self.make_error("%s element must be numeric type" % self.name)
        else:
            raise self.make_error("the %s operator requires an array of two elements" % self.name)

    def eval(self, document):
        v1 = self.value[0]
        if isinstance(v1, ProjectOperator):
            v1 = v1.eval(document)
        elif Value.is_doc_ref_key(v1):
            v1 = document.get(v1[1:])
        v2 = self.value[1]
        if isinstance(v2, ProjectOperator):
            v2 = v2.eval(document)
        elif Value.is_doc_ref_key(v2):
            v2 = document.get(v2[1:])

        if (v1 == undefined or v1 == None) or (v2 == undefined or v2 == None):
            return None
        elif isNumberType(v1) and isNumberType(v2):
            return self.compute(float(v1), float(v2))
        else:
            raise self.make_error("%s only supports numeric types" % self.name)

    def compute(self, v1, v2):
        raise NotImplementedError()


class ProjectAddOperator(ProjectDualNumberOperator):

    name = "$add"

    def compute(self, v1, v2):
        return v1 + v2


class ProjectSubstractOperator(ProjectDualNumberOperator):

    name = "$substract"

    def compute(self, v1, v2):
        return v1 - v2


class ProjectMultiplyOperator(ProjectDualNumberOperator):

    name = "$multiply"

    def compute(self, v1, v2):
        return v1 * v2


class ProjectDivideOperator(ProjectDualNumberOperator):

    name = "$divide"

    def compute(self, v1, v2):
        return v1 / v2


class ProjectModOperator(ProjectDualNumberOperator):

    name = "$mod"

    def compute(self, v1, v2):
        return v1 % v2



class ProjectConvertOperator(ProjectOperator):

    def __init__(self, key, value):
        super(ProjectConvertOperator, self).__init__(key, value)
        if not Value.is_doc_ref_key(value):
            if isinstance(value, dict):
                if len(value) == 1:
                    self.value = OperatorFactory.new_project(key, value)
                else:
                    raise self.make_error("%s parameter operator must contain exactly one field" % self.name)
            else:
                raise self.make_error("%s field path references must be prefixed with a '$'" % self.name)

    def eval (self, document):
        v = self.value
        if isinstance(v, ProjectOperator):
            v = v.eval(document)
        elif Value.is_doc_ref_key(v):
            v = document.get(v[1:])
        return self.convert(v)

    def convert(self, v):
        raise NotImplementedError()


class ProjectToLowerOperator(ProjectConvertOperator):

    name = "$toLower"

    def convert(self, v):
        if v == undefined or v == None:
            return ""
        return string.lower(v)


class ProjectToUpperOperator(ProjectConvertOperator):

    name = "$toUpper"

    def convert(self, v):
        if v == undefined or v == None:
            return ""
        return string.upper(v)


class ProjectToNumberOperator(ProjectConvertOperator):

    name = "$toNumber"

    def convert(self, v):
        if v == undefined or v == None:
            return 0
        return float(v)


class ProjectConcatOperator(ProjectOperator):

    name = "$concat"

    def __init__(self, key, value):
        super(ProjectConcatOperator, self).__init__(key, value)
        if isinstance(value, list) and len(value) > 2:
            for i, item in enumerate(value):
                if not isinstance(item, basestring):
                    if isinstance(item, dict):
                        if len(item) == 1:
                            self.value[i] = OperatorFactory.new_project(key, item)
                        else:
                            raise self.make_error("$concat parameter operator must contain exactly one field")
                    else:
                        raise self.make_error("$concat field path references must be prefixed with a '$'")
        else:
            raise self.make_error("the $concat operator requires an array of at least two elements")

    def eval(self, document):
        rets = []
        for v in self.value:
            if isinstance(v, ProjectOperator):
                rv = v.eval(document)
            elif Value.is_doc_ref_key(v):
                rv = document.get(v[1:])
            else:
                rv = v
            if rv is None or rv == undefined:
                return None
            elif not isinstance(rv, basestring):
                raise self.make_error("$concat runtime error: only supports strings")
            else:
                rets.append(rv)
        return "".join(rets)



class ProjectDateOperator(ProjectOperator):

    def __init__(self, key, value):
        super(ProjectDateOperator, self).__init__(key, value)
        if not Value.is_doc_ref_key(value):
            if isinstance(value, dict):
                if len(value) == 1:
                    self.value = OperatorFactory.new_project(key, value)
                else:
                    raise self.make_error("%s parameter operator must contain exactly one field" % self.name)
            else:
                raise self.make_error("%s field path references must be prefixed with a '$'" % self.name)

    def eval(self, document):
        d = self._make_date(document)
        return self._eval(d)

    def _make_date(self, document):
        v = self.value
        if isinstance(self.value, ProjectOperator):
            v = self.value.eval(document)
        elif Value.is_doc_ref_key(self.value):
            v = document.get(self.value[1:])

        if isinstance(v, datetime):
            return v
        elif isinstance(v, date):
            return datetime(v.year, v.month, v.day)
        elif isNumberType(v):
            return datetime.fromtimestamp(float(v))
        else:
            raise self.make_error("%s value must be date type" % self.name)


class ProjectDayOfYearOperator(ProjectDateOperator):

    name = "$dayOfYear"

    def _eval(self, d):
        dy = datetime(d.year, 1, 1)
        return (d-dy).days


class ProjectDayOfMonthOperator(ProjectDateOperator):

    name = "$dayOfMonth"

    def _eval(self, d):
        return d.day


class ProjectDayOfWeekOperator(ProjectDateOperator):

    name = "$dayOfWeek"

    def _eval(self, d):
        return d.weekday() + 1


class ProjectYearOperator(ProjectDateOperator):

    name = "$year"

    def _eval(self, d):
        return d.year


class ProjectMonthOperator(ProjectDateOperator):

    name = "$month"

    def _eval(self, d):
        return d.month


class ProjectHourOperator(ProjectDateOperator):

    name = "$hour"

    def _eval(self, d):
        return d.hour


class ProjectMinuteOperator(ProjectDateOperator):

    name = "$minute"

    def _eval(self, d):
        return d.minute


class ProjectSecondOperator(ProjectDateOperator):

    name = "$second"

    def _eval(self, d):
        return d.second


class ProjectMillisecondOperator(ProjectDateOperator):

    name = "$millisecond"

    def _eval(self, d):
        return d.microsecond // 1000


class ProjectCallOperator(ProjectOperator):

    name = "$call"

    def __init__(self, key, value):
        super(ProjectCallOperator, self).__init__(key, value)
        if not callable(value):
            raise self.make_error("the $call operator requires callable")

    def eval(self, document):
        return self.value(document)


class ProjectCombineOperator(ProjectOperator):

    def __init__(self, key, value):
        super(ProjectCombineOperator, self).__init__(key, value)
        combined_ops = {}
        for k, v in value.iteritems():
            combined_ops[k] = OperatorFactory.new_project(k, v)
        self.combined_ops = combined_ops

    def eval(self, document):
        pv = Document()
        for k, combine_op in self.combined_ops.iteritems():
            v = combine_op.project(document)
            if v != undefined:
                pv.set(k, v)
        return dict(pv)


class GroupOperator(Operator):

    command = "$group"

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def group(self, document, acc_val):
        try:
            return self.eval(document, acc_val)
        except Exception, e:
            raise self.make_error("%s runtime error: %s" % (self.name, str(e)))

    def eval(self, document, acc_val):
        raise NotImplementedError()


class GroupUnaryOperator(GroupOperator):

    def __init__(self, key, value):
        super(GroupUnaryOperator, self).__init__(key, value)
        if not Value.is_doc_ref_key(value):
            if isinstance(value, dict):
                if len(value) == 1:
                    self.value = OperatorFactory.new_project(key, value)
                else:
                    raise self.make_error("aggregating group operator must contain exactly one field")
            elif isinstance(value, collections.Iterable):
                raise self.make_error("aggregating group operators are unary (%s)" % self.name)


    def get_value(self, document, default=None):
        v = self.value
        if isinstance(self.value, ProjectOperator):
            v = self.value.eval(document)
            if v == undefined:
                v = default
        elif Value.is_doc_ref_key(self.value):
            v = document.get(self.value[1:], default)
        return v


class GroupSumOperator(GroupUnaryOperator):

    name = "$sum"

    def eval(self, document, acc_val):
        value = self.get_value(document)
        if acc_val == undefined:
            acc_val = 0
        if isNumberType(value):
            return acc_val + value
        else:
            return acc_val


class GroupMinOperator(GroupUnaryOperator):

    name = "$min"

    def eval(self, document, acc_val):
        value = self.get_value(document, undefined)
        if value < acc_val:
            return value
        else:
            return acc_val


class GroupMaxOperator(GroupUnaryOperator):

    name = "$max"

    def eval(self, document, acc_val):
        value = self.get_value(document, undefined)
        if value > acc_val:
            return value
        else:
            return acc_val


class GroupFirstOperator(GroupUnaryOperator):

    name = "$first"

    def eval(self, document, acc_val):
        if acc_val == undefined:
            return self.get_value(document)
        return acc_val


class GroupLastOperator(GroupUnaryOperator):

    name = "$last"

    def eval(self, document, acc_val):
        return self.get_value(document)


class GroupAddToSetOperator(GroupUnaryOperator):

    name = "$addToSet"

    def eval(self, document, acc_val):
        if acc_val == undefined:
            acc_val = []
        value = self.get_value(document, undefined)
        if value != undefined and value not in acc_val:
            acc_val.append(value)
        return acc_val


class GroupPushOperator(GroupUnaryOperator):

    name = "$push"

    def eval(self, document, acc_val):
        if acc_val == undefined:
            acc_val = []
        value = self.get_value(document, undefined)
        if value != undefined:
            acc_val.append(value)
        return acc_val


class GroupConcatToSetOperator(GroupUnaryOperator):

    name = "$concatToSet"

    def eval(self, document, acc_val):
        if acc_val == undefined:
            acc_val = []
        value = self.get_value(document, undefined)
        if value != undefined:
            acc_val = list(set(acc_val + list(value)))
        return acc_val


class GroupConcatToListOperator(GroupUnaryOperator):

    name = "$concatToList"

    def eval(self, document, acc_val):
        if acc_val == undefined:
            acc_val = []
        value = self.get_value(document, undefined)
        if value != undefined:
            acc_val = acc_val + list(value)
        return acc_val


class GroupCallOperator(GroupUnaryOperator):

    name = "$call"

    def __init__(self, key, value):
        super(GroupCallOperator, self).__init__(key, value)
        if not callable(value):
            raise self.make_error("the $call operator requires callable")

    def eval(self, document, acc_val):
        return self.value(document, acc_val)


class GroupCombineOperator(GroupOperator):

    def __init__(self, key, value):
        super(GroupCombineOperator, self).__init__(key, value)
        combined_ops = {}
        for k, v in value.iteritems():
            combined_ops[k] = OperatorFactory.new_group(k, v)
        self.combined_ops = combined_ops

    def eval(self, document, acc_val):
        if acc_val is undefined:
            acc_val = Document()
        pv = Document()
        for k, combine_op in self.combined_ops.iteritems():
            v = combine_op.group(document, acc_val.get(k, undefined))
            if v == undefined:
                v = None
            pv.set(k, v)
        return dict(pv)
