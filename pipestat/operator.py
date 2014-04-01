# -*- coding: utf-8 -*-

import re
import time
import string
import collections
from pipestat.errors import OperatorError, CommandError
from pipestat.utils import Value
from datetime import datetime, date


_operators = {}


class OperatorFactory(object):

    @staticmethod
    def new_match(key, value):
        if key == "$and":
            return MatchAndOperator(value)
        elif key == "$or":
            return MatchOrOperator(value)

        if not isinstance(value, dict):
            return MatchEqualOperator(key, value)
        else:
            if len(value) == 1:
                name = value.keys()[0]
                match_operators = _operators.get("$match", {})
                if name in match_operators:
                    return match_operators[name](key, value[name])
            elif len(value) > 1:
                return MatchCombineOperator(key, value)

        raise CommandError("the $match command with invalid operator for '%s'" % key, "$match")

    @staticmethod
    def new_project(key, value):
        if not isinstance(value, dict):
            return ProjectValueOperator(key, value)
        else:
            if len(value) == 1:
                name = value.keys()[0]
                if Value.is_operator(name):
                    project_operators = _operators.get("$project", {})
                    if name in project_operators:
                        return project_operators[name](key, value[name])
            if value:
                return ProjectCombineOperator(key, value)

        raise CommandError("the $project command with invalid operator for '%s'" % key, "$project")

    @staticmethod
    def new_group(key, value):
        if isinstance(value, dict):
            if len(value) == 1:
                name = value.keys()[0]
                group_operators = _operators.get("$group", {})
                if name in group_operators:
                    return group_operators[name](key, value[name])

            if value:
                return GroupCombineOperator(key, value)

        raise CommandError("the $group command with invalid operator for '%s'" % key, "$group")

    @staticmethod
    def new_expression(key, value):
        if len(value) == 1:
            name = value.keys()[0]
            if Value.is_operator(name):
                project_operators = _operators.get("$project", {})
                if name in project_operators:
                    return project_operators[name](key, value[name])
        if value:
            return ProjectCombineOperator(key, value)

        raise CommandError("the $group with invalid expressions for '%s'" % key, "$group")


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
        except Exception, e:
            raise self.make_error("%s: runtime error '%s'" % (self.name, str(e)))

    def eval(self, document):
        raise NotImplemented()


class MatchKeyCmpOperator(MatchOperator):

    def __init__(self, key, value):
        self.key = key
        self.value = value


class MatchRegexpOperator(MatchKeyCmpOperator):

    name = "$regexp"

    def __init__(self, key, value):
        super(MatchRegexpOperator, self).__init__(key, value)
        try:
            self.pat = re.compile(value)
        except Exception:
            raise self.make_error("the $regexp operator requires regular expression")

    def eval(self, document):
        doc_val = document.get(self.key, default="")
        m = self.pat.search(doc_val)
        if m:
            return True
        return False


class MatchNumberCmpOperator(MatchKeyCmpOperator):

    def __init__(self, key, value):
        super(MatchNumberCmpOperator, self).__init__(key, value)
        if not Value.is_doc_ref_key(value):
            try:
                self.value = float(value)
            except Exception:
                raise self.make_error("the %s operator requires key-ref or numeric type" % self.name)

    def eval(self, document):
        doc_val = float(document.get(self.key))
        if Value.is_doc_ref_key(self.value):
            value = float(document.get(self.value[1:]))
            return self.cmp(doc_val, value)
        else:
            return self.cmp(doc_val, self.value)

    def cmp(self, doc_val, value):
        raise NotImplementedError()


class MatchLTOperator(MatchNumberCmpOperator):

    name = "$lt"

    def cmp(self, doc_val, value):
        return doc_val < value


class MatchLTEOperator(MatchNumberCmpOperator):

    name = "$lte"

    def cmp(self, doc_val, value):
        return doc_val <= value


class MatchGTOperator(MatchNumberCmpOperator):

    name = "$gt"

    def cmp(self, doc_val, value):
        return doc_val > value


class MatchGTEOperator(MatchNumberCmpOperator):

    name = "$gte"

    def cmp(self, doc_val, value):
        return doc_val >= value


class MatchEqualOperator(MatchKeyCmpOperator):

    name = "$eq"

    def eval(self, document):
        doc_val = document.get(self.key)
        if Value.is_doc_ref_key(self.value):
            value = document.get(self.value[1:])
            return doc_val == value
        else:
            return doc_val == self.value


class MatchNotEqualOperator(MatchKeyCmpOperator):

    name = "$ne"

    def eval(self, document):
        doc_val = document.get(self.key)
        if Value.is_doc_ref_key(self.value):
            value = document.get(self.value[1:])
            return doc_val != value
        else:
            return doc_val != self.value


class MatchBelongOperator(MatchKeyCmpOperator):

    def __init__(self, key, value):
        super(MatchBelongOperator, self).__init__(key, value)
        if not Value.is_doc_ref_key(value):
            if isinstance(value, collections.Iterable):
                super(MatchBelongOperator, self).__init__(key, value)
            else:
                raise self.make_error("the %s operator requires key-ref or iterable" % self.name)

    def eval(self, document):
        doc_val = document.get(self.key)
        if Value.is_doc_ref_key(self.value):
            value = document.get(self.value[1:])
            return self.belong(doc_val, value)
        else:
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


class MatchCallOperator(MatchKeyCmpOperator):

    name = "$call"

    def __init__(self, key, value):
        super(MatchCallOperator, self).__init__(key, value)
        if not callable(value):
            raise self.make_error("the $call operator requires callable")

    def eval(self, document):
        doc_val = document.get(self.key)
        if self.value(doc_val, document):
            return True
        return False


class MatchLogicOperator(MatchOperator):

    def __init__(self, value):
        self.value = value
        if isinstance(value, list):
            sub_ops = []
            for sub_op in value:
                if isinstance(sub_op, dict) and len(sub_op) == 1:
                    sub_op_key = sub_op.keys()[0]
                    sub_op_val = sub_op[sub_op_key]
                    sub_ops.append(OperatorFactory.new_match(sub_op_key, sub_op_val))
                else:
                    raise self.make_error('%s: item must be nested match operator' % self.name)
            self.sub_ops = sub_ops
        else:
            raise self.make_error('the %s operator requires an array of operand(s)' % self.name)


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


class MatchCombineOperator(MatchKeyCmpOperator):

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
            return {self.key: self.eval(document)}
        except Exception, e:
            raise self.make_error("%s: runtime error '%s'" % (self.name, str(e)))

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
            return document.get(self.value[1:])
        elif self.value == 1:
            return document.get(self.key)


class ProjectExtractOperator(ProjectOperator):

    name = "$extract"

    def __init__(self, key, value):
        super(ProjectExtractOperator, self).__init__(key, value)
        if isinstance(value, list) and len(value) == 2:
            if not Value.is_doc_ref_key(value[0]):
                if isinstance(value[0], dict) and len(value[0]) == 1:
                    try:
                        self.value[0] = OperatorFactory.new_project(key, value[0])
                    except Exception:
                        raise self.make_error("$extract: string must be key-ref or nested operator")
                else:
                    raise self.make_error("$extract: string must be key-ref or nested operator")
            try:
                self.value[1] = re.compile(value[1])
            except Exception:
                raise self.make_error("$extract: pattern must be regular expression")
        else:
            raise self.make_error("the $extract operator requires an array of 2 operands")

    def eval(self, document):
        v = self.value[0]
        if isinstance(v, ProjectOperator):
            v = v.eval(document)
        elif Value.is_doc_ref_key(v):
            v = document.get(v[1:], default="")

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
                if isinstance(value[0], dict) and len(value[0]) == 1:
                    try:
                        self.value[0] = OperatorFactory.new_project(key, value[0])
                    except Exception:
                        raise self.make_error("$extract: string must be key-ref or nested operator")
                else:
                    raise self.make_error("$extract: string must be key-ref or nested operator")
            try:
                time.strftime(value[1])
            except Exception:
                raise self.make_error("$extract: format must be string type")
        else:
            raise self.make_error("the $extract operator requires an array of 2 operands")

    def eval(self, document):
        v = self.value[0]
        if isinstance(v, ProjectOperator):
            v = v.eval(document)
        elif Value.is_doc_ref_key(v):
            v = document.get(v[1:], default="")
        return time.mktime(time.strptime(v, self.value[1]))


class ProjectDualNumberOperator(ProjectOperator):

    def __init__(self, key, value):
        super(ProjectDualNumberOperator, self).__init__(key, value)
        if isinstance(value, list) and len(value) == 2:
            for i, v in enumerate(value):
                if isinstance(v, dict) and len(v) == 1:
                    try:
                        self.value[i] = OperatorFactory.new_project(key, v)
                    except Exception:
                        raise self.make_error("%s: operand must be key-ref or nested operator or numeric type" % self.name)
                elif not Value.is_doc_ref_key(v):
                    try:
                        self.value[i] = float(v)
                    except Exception:
                        raise self.make_error("%s: operand must be key-ref or nested operator or numeric type" % self.name)
        else:
            raise self.make_error("the %s requires an array of 2 operands" % self.name)

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
        return self.compute(float(v1), float(v2))

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



class ProjectConvertStrOperator(ProjectOperator):

    def __init__(self, key, value):
        super(ProjectConvertStrOperator, self).__init__(key, value)
        if not Value.is_doc_ref_key(value):
            if isinstance(value, dict) and len(value) == 1:
                try:
                    self.value = OperatorFactory.new_project(key, value)
                except Exception:
                    raise self.make_error("the %s requires key-ref or nested operator" % self.name)
            else:
                raise self.make_error("the %s requires key-ref or nested operator" % self.name)

    def eval (self, document):
        v = self.value
        if isinstance(v, ProjectOperator):
            v = v.eval(document)
        elif Value.is_doc_ref_key(v):
            v = document.get(v[1:])
        return self.convert(v)

    def convert(self, v):
        raise NotImplementedError()


class ProjectToLowerOperator(ProjectConvertStrOperator):

    name = "$toLower"

    def convert(self, v):
        if v is not None:
            return string.lower(v)


class ProjectToUpperOperator(ProjectConvertStrOperator):

    name = "$toUpper"

    def convert(self, v):
        if v is not None:
            return string.upper(v)


class ProjectDateOperator(ProjectOperator):

    def __init__(self, key, value):
        super(ProjectDateOperator, self).__init__(key, value)
        if not Value.is_doc_ref_key(value):
            if isinstance(value, dict) and len(value) == 1:
                try:
                    self.value = OperatorFactory.new_project(key, value)
                except Exception:
                    raise self.make_error("the %s requires key-ref or nested operator" % self.name)
            else:
                raise self.make_error("the %s requires key-ref or nested operator" % self.name)

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
        else:
            return datetime.fromtimestamp(float(v))


class ProjectDayOfYearOperator(ProjectDateOperator):

    name = "$dayOfYear"

    def eval(self, document):
        d1 = self._make_date(document)
        d2 = datetime(d1.year, 1, 1)
        return (d1-d2).days


class ProjectDayOfMonthOperator(ProjectDateOperator):

    name = "$dayOfMonth"

    def eval(self, document):
        d = self._make_date(document)
        return d.day


class ProjectDayOfWeekOperator(ProjectDateOperator):

    name = "$dayOfWeek"

    def eval(self, document):
        d = self._make_date(document)
        return d.weekday() + 1


class ProjectYearOperator(ProjectDateOperator):

    name = "$year"

    def eval(self, document):
        d = self._make_date(document)
        return d.year


class ProjectMonthOperator(ProjectDateOperator):

    name = "$month"

    def eval(self, document):
        d = self._make_date(document)
        return d.month


class ProjectHourOperator(ProjectDateOperator):

    name = "$hour"

    def eval(self, document):
        d = self._make_date(document)
        return d.hour


class ProjectMinuteOperator(ProjectDateOperator):

    name = "$minute"

    def eval(self, document):
        d = self._make_date(document)
        return d.minute


class ProjectSecondOperator(ProjectDateOperator):

    name = "$second"

    def eval(self, document):
        d = self._make_date(document)
        return d.second


class ProjectMillisecondOperator(ProjectDateOperator):

    name = "$millisecond"

    def eval(self, document):
        d = self._make_date(document)
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
        combined_ops = []
        for k, v in value.iteritems():
            combined_ops.append(OperatorFactory.new_project(k, v))
        self.combined_ops = combined_ops

    def eval(self, document):
        pv = {}
        for combine_op in self.combined_ops:
            pv.update(combine_op.project(document))
        return pv


class GroupOperator(Operator):

    command = "$group"

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def group(self, document, acc_val):
        try:
            return self.eval(document, acc_val)
        except Exception, e:
            raise self.make_error("%s: runtime error '%s'" % (self.name, str(e)))

    def eval(self, document, acc_val):
        raise NotImplementedError()


class GroupSumOperator(GroupOperator):

    name = "$sum"

    def __init__(self, key, value):
        super(GroupSumOperator, self).__init__(key, value)
        if not Value.is_doc_ref_key(value):
            try:
                self.value = float(value)
            except Exception:
                raise self.make_error("the $sum operator requires key-ref or numeric type")

    def eval(self, document, acc_val):
        value = self.value
        if Value.is_doc_ref_key(value):
            value = document.get(value[1:])
            if value is not None:
                value = float(value)
        if acc_val is None:
            acc_val = 0
        if value is not None:
            return acc_val + value
        else:
            return acc_val


class GroupRefValueOperator(GroupOperator):

    def __init__(self, key, value):
        super(GroupRefValueOperator, self).__init__(key, value)
        if not Value.is_doc_ref_key(value):
            raise self.make_error("the %s operator requires key-ref" % self.name)


class GroupMinOperator(GroupRefValueOperator):

    name = "$min"

    def eval(self, document, acc_val):
        value = document.get(self.value[1:])
        if value is not None:
            value = float(value)
        if acc_val is None:
            return value
        elif value is not None:
            return min(acc_val, value)
        else:
            return acc_val


class GroupMaxOperator(GroupRefValueOperator):

    name = "$max"

    def eval(self, document, acc_val):
        value = document.get(self.value[1:])
        if value is not None:
            value = float(value)
        if acc_val is None:
            return value
        elif value is not None:
            return max(acc_val, value)
        else:
            return acc_val


class GroupFirstOperator(GroupRefValueOperator):

    name = "$first"

    def eval(self, document, acc_val):
        if acc_val is not None:
            return acc_val
        return document.get(self.value[1:])


class GroupLastOperator(GroupRefValueOperator):

    name = "$last"

    def eval(self, document, acc_val):
        value = document.get(self.value[1:])
        if value is not None:
            return value
        return acc_val


class GroupAddToSetOperator(GroupRefValueOperator):

    name = "$addToSet"

    def eval(self, document, acc_val):
        if acc_val is None:
            acc_val = []
        value = document.get(self.value[1:])
        if (value is not None) and (value not in acc_val):
            acc_val.append(value)
        return acc_val


class GroupPushOperator(GroupRefValueOperator):

    name = "$push"

    def eval(self, document, acc_val):
        if acc_val is None:
            acc_val = []
        value = document.get(self.value[1:])
        if value is not None:
            acc_val.append(value)
        return acc_val


class GroupConcatToSetOperator(GroupRefValueOperator):

    name = "$concatToSet"

    def eval(self, document, acc_val):
        if acc_val is None:
            acc_val = []
        value = document.get(self.value[1:])
        if value is not None:
            return list(set(acc_val + list(value)))
        return acc_val


class GroupConcatToListOperator(GroupRefValueOperator):

    name = "$concatToList"

    def eval(self, document, acc_val):
        if acc_val is None:
            acc_val = []
        value = document.get(self.value[1:])
        if value is not None:
            return acc_val + list(value)
        return acc_val


class GroupCombineOperator(GroupOperator):

    def __init__(self, key, value):
        super(GroupCombineOperator, self).__init__(key, value)
        combined_ops = {}
        for k, v in value.iteritems():
            combined_ops[k] = OperatorFactory.new_group(k, v)
        self.combined_ops = combined_ops

    def eval(self, document, acc_val):
        acc_val = acc_val or {}
        pv = {}
        for k, combine_op in self.combined_ops.iteritems():
            pv[k] = combine_op.group(document, acc_val.get(k))
        return pv
