# -*- coding: utf-8 -*-

import re
import time
import string
import types
from datetime import datetime, date
from pipestat.errors import PipelineError, CommandError, OperatorError
from pipestat.utils import Value, isNumberType
from pipestat.models import Document, undefined
from pipestat.constants import NumberTypes, DateTypes, ArrayTypes
from pipestat.constants import (
    VALUE_TYPE_PLAIN, VALUE_TYPE_REFKEY, VALUE_TYPE_OPERATOR
)
from pipestat.parse import Parser


_operators = {}


class OperatorFactory(object):

    @staticmethod
    def new_match(key, value):
        if key == "$and":
            return MatchAndOperator(value)
        elif key == "$or":
            return MatchOrOperator(value)
        elif key == "$nor":
            return MatchNorOperator(value)
        elif key == "$call":
            return MatchCallOperator(value)

        if not isinstance(value, dict):
            return MatchEqualOperator(key, value)
        else:
            if len(value) == 1:
                name = value.keys()[0]
                match_operators = _operators.get("$match", {})
                if name in ["$and", "$or", "$nor", "$call"]:
                    raise CommandError("invalid operator '%s'" % name)
                if name in match_operators:
                    return match_operators[name](key, value[name])
                else:
                    raise CommandError("unknow $match operator '%s'" % name, "$match")
            else:
                return MatchCombineOperator(key, value)

    @staticmethod
    def new_project(key, value, expr=False):
        if not isinstance(value, dict):
            return ProjectValueOperator(key, value, expr=expr)
        else:
            if len(value) == 1:
                name = value.keys()[0]
                project_operators = _operators.get("$project", {})
                if Value.is_operator(name):
                    if name in project_operators:
                        return project_operators[name](key, value[name], expr=expr)
                    else:
                        raise CommandError("unknow $project operator '%s'" % name, "$project")
            return ProjectCombineOperator(key, value, expr=expr)

    @staticmethod
    def new_group(key, value):
        if not isinstance(value, dict):
            raise PipelineError("the $group aggregate field '%s' must be defined as an expression inside an object" % key)

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
            raise self.make_error("%s runtime error: %s" % (self.name, e.message))

    def eval(self, document):
        raise NotImplemented()


class MatchElemOperator(MatchOperator):

    def __init__(self, value):
        self.operators = []
        if isinstance(value, dict):
            for k, v in value.iteritems():
                self.operators.append(OperatorFactory.new_match(k, v))
        else:
            raise self.make_error('element match must be object')

    def match(self, document):
        for op in self.operators:
            if not op.match(document):
                return False
        return True


class MatchKeyOperator(MatchOperator):

    def __init__(self, key, value):
        self.key = key
        self.value = value


class MatchKeyElemOperator(MatchKeyOperator):

    def eval(self, document):
        doc_val = document.get(self.key, undefined)
        if isinstance(doc_val, ArrayTypes):
            for v in doc_val:
                m = self._eval_val(v, document)
                if m:
                    return True
            return False
        else:
            return self._eval_val(doc_val, document)


class MatchExistsOperator(MatchKeyOperator):

    name = "$exists"

    def __init__(self, key, value):
        super(MatchExistsOperator, self).__init__(key, value)
        if not value in [0, 1, True, False]:
            self.make_error("the $exists operator requires bool")

    def eval(self, document):
        doc_val = document.get(self.key, undefined)
        if self.value and doc_val != undefined:
            return True
        elif not self.value and doc_val == undefined:
            return True
        return False


class MatchRegexOperator(MatchKeyElemOperator):

    name = "$regex"

    def __init__(self, key, value):
        super(MatchRegexOperator, self).__init__(key, value)
        if isinstance(value, basestring):
            self.value = re.compile(value)
        elif hasattr(getattr(value, "search", None), "__call__"):
            self.value = value
        else:
            raise self.make_error("the $regex operator requires regular expression")

    def _eval_val(self, doc_val, document):
        if not isinstance(doc_val, basestring):
            return False
        m = self.value.search(doc_val)
        if m:
            return True
        return False


class MatchModOperator(MatchKeyElemOperator):

    name = "$mod"

    def __init__(self, key, value):
        super(MatchModOperator, self).__init__(key, value)
        if not isinstance(value, ArrayTypes):
            self.make_error("the $mod operator requires array type")

        vcount = len(value)
        if vcount < 2:
            self.make_error("BadValue malformed $mod, not enough elements")
        elif vcount > 2:
            self.make_error("BadValue malformed $mod, too many elements")

        if not all(map(isNumberType, value)):
            self.make_error("BadValue malformed $mod, elem value should be number")
        value = map(int, value)
        if value[0] == 0:
            self.make_error("$mod can't be 0")
        self.value = value

    def _eval_val(self, doc_val, document):
        if not isNumberType(doc_val):
            return False
        if int(doc_val) % self.value[0] == self.value[1]:
            return True
        return False


class MatchCmpOperator(MatchKeyElemOperator):

    def _eval_val(self, doc_val, document):
        return self.cmp(doc_val, self.value)

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


class MatchBelongOperator(MatchKeyElemOperator):

    def __init__(self, key, value):
        super(MatchBelongOperator, self).__init__(key, value)
        if isinstance(value, ArrayTypes):
            super(MatchBelongOperator, self).__init__(key, value)
        else:
            raise self.make_error("the %s operator requires iterable" % self.name)

    def _eval_val(self, doc_val, document):
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


class MatchAllOperator(MatchKeyOperator):

    name = "$all"

    def __init__(self, key, value):
        super(MatchAllOperator, self).__init__(key, value)
        if not isinstance(value, ArrayTypes):
            raise self.make_error("thie $all operator require array")

    def eval(self, document):
        doc_val = document.get(self.key, undefined)
        if doc_val == undefined:
            return False
        if not isinstance(doc_val, ArrayTypes):
            doc_val = [doc_val]
        for v in self.value:
            if v not in doc_val:
                return False
        return True



class MatchElemMatchOperator(MatchKeyOperator):

    name = "$elemMatch"

    def __init__(self, key, value):
        super(MatchElemMatchOperator, self).__init__(key, value)
        if isinstance(value, dict):
            self.value = MatchElemOperator(value)
        else:
            raise self.make_error("the $elemMatch operator requires an object")

    def eval(self, document):
        doc_val = document.get(self.key, undefined)
        if isinstance(doc_val, ArrayTypes):
            for v in doc_val:
                if isinstance(v, dict):
                    m = self.value.match(v)
                    if m:
                        return True
            return False
        else:
            return False


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


class MatchLogicOperator(MatchOperator):

    def __init__(self, value):
        self.value = value
        if isinstance(value, list):
            sub_ops = []
            for sub_op in value:
                sub_ops.append(MatchElemOperator(sub_op))
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


class MatchNorOperator(MatchLogicOperator):

    name = "$nor"

    def eval(self, document):
        for sub_op in self.sub_ops:
            if sub_op.match(document):
                return False
        return True


class MatchNotOperator(MatchKeyOperator):

    name = "$not"

    def __init__(self, key, value):
        super(MatchNotOperator, self).__init__(key, value)
        if isinstance(value, dict):
            self.value = OperatorFactory.new_match(key, value)
        else:
            self.make_error("invalid use of $not")

    def eval(self, document):
        if self.value.match(document):
            return False
        else:
            return True


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

    def __init__(self, key, value, expr=False):
        self.key = key
        self.value = value
        self.expr = expr
        self.value_type = VALUE_TYPE_PLAIN

    def project(self, document):
        try:
            return self.eval(document)
        except OperatorError:
            raise
        except Exception, e:
            raise self.make_error("%s runtime error: %s" % (self.name, e.message))

    def eval(self, document):
        raise NotImplemented()


class ProjectValueOperator(ProjectOperator):

    name = "$value"
    returnTypes = None

    def __init__(self, key, value, expr=False):
        super(ProjectValueOperator, self).__init__(key, value, expr=expr)
        if Value.is_doc_ref_key(value):
            self.value = value[1:]
        elif value == 1:
            self.value = key
        else:
            raise self.make_error("field path references must be prefixed with a '$'")

        if self.expr and value == 1:
            raise self.make_error("field inclusion is not allowed inside of $expressions")

    def eval(self, document):
        return document.get(self.value, undefined)


class ProjectExtractOperator(ProjectOperator):

    name = "$extract"
    returnTypes = types.StringTypes

    def __init__(self, key, value, expr=False):
        super(ProjectExtractOperator, self).__init__(key, value, expr=expr)
        if isinstance(value, list) and len(value) == 2:
            if Value.is_doc_ref_key(value[0]):
                self.value[0] = value[0][1:]
                self.value_type = VALUE_TYPE_REFKEY
            else:
                if isinstance(value[0], dict):
                    prj = OperatorFactory.new_project(key, value[0], expr=True)
                    if (not prj.returnTypes) or (set(prj.returnTypes) & set(types.StringTypes)):
                        self.value[0] = prj
                        self.value_type = VALUE_TYPE_OPERATOR
                    else:
                        raise self.make_error("$extract source must be string type")
                elif not isinstance(value[0], basestring):
                    raise self.make_error("$extract source must be string type")
            try:
                self.value[1] = re.compile(value[1])
            except Exception:
                raise self.make_error("$extract pattern must be regular expression")
        else:
            raise self.make_error("the $extract operator requires an array of two elements")

    def eval(self, document):
        v = self.value[0]
        if self.value_type == VALUE_TYPE_REFKEY:
            v = document.get(v, undefined)
        elif self.value_type == VALUE_TYPE_OPERATOR:
            v = v.eval(document)

        if not isinstance(v, basestring):
            raise self.make_error("$extract source must be string type")
        else:
            m = self.value[1].search(v)
            if m:
                if self.key in m.groupdict():
                    return m.groupdict()["extract"]
                elif len(m.groups()) > 0:
                    return m.group(1)
                else:
                    return m.group()


class ProjectTimestampOperator(ProjectOperator):

    name = "$timestamp"
    returnTypes = [types.IntType, types.LongType, types.FloatType]

    def __init__(self, key, value, expr=False):
        super(ProjectTimestampOperator, self).__init__(key, value, expr=expr)
        if isinstance(value, list) and len(value) == 2:
            if Value.is_doc_ref_key(value[0]):
                self.value[0] = value[0][1:]
                self.value_type = VALUE_TYPE_REFKEY
            else:
                if isinstance(value[0], dict):
                    prj = OperatorFactory.new_project(key, value[0], expr=True)
                    if (not prj.returnTypes) or (set(prj.returnTypes) & set(types.StringTypes)):
                        self.value[0] = prj
                        self.value_type = VALUE_TYPE_OPERATOR
                    else:
                        raise self.make_error("$timestamp source must be string type")
                elif not isinstance(value[0], basestring):
                    raise self.make_error("$timestamp source must be string type")

            if not isinstance(value[1], basestring):
                raise self.make_error("$timestamp format must be string type")
        else:
            raise self.make_error("the $timestamp operator requires an array of two elements")

    def eval(self, document):
        v = self.value[0]
        if self.value_type == VALUE_TYPE_REFKEY:
            v = document.get(v)
        elif self.value_type == VALUE_TYPE_OPERATOR:
            v = v.eval(document)

        if not isinstance(v, basestring):
            raise self.make_error("$timestamp source must be string type")
        else:
            return time.mktime(time.strptime(v, self.value[1]))


class ProjectDualNumberOperator(ProjectOperator):

    def __init__(self, key, value, expr=False):
        super(ProjectDualNumberOperator, self).__init__(key, value, expr=expr)
        if isinstance(value, list) and len(value) == 2:
            self.value_type = [VALUE_TYPE_PLAIN, VALUE_TYPE_PLAIN]
            for i, v in enumerate(value):
                if Value.is_doc_ref_key(v):
                    self.value[i] = v[1:]
                    self.value_type[i] = VALUE_TYPE_REFKEY
                else:
                    if isinstance(v, dict):
                        prj = OperatorFactory.new_project(key, v, expr=True)
                        if (not prj.returnTypes) or (set(prj.returnTypes) & set(NumberTypes)):
                            self.value[i] = prj
                            self.value_type[i] = VALUE_TYPE_OPERATOR
                        else:
                            raise self.make_error("%s element must be numeric type" % self.name)
                    elif isNumberType(v):
                        self.value[i] = float(v)
                    else:
                        raise self.make_error("%s element must be numeric type" % self.name)
        else:
            raise self.make_error("the %s operator requires an array of two elements" % self.name)

    def eval(self, document):
        v1, v2 = self.value
        vt1, vt2 = self.value_type

        if vt1 == VALUE_TYPE_REFKEY:
            v1 = document.get(v1)
        elif vt1 == VALUE_TYPE_OPERATOR:
            v1 = v1.eval(document)

        if vt2 == VALUE_TYPE_REFKEY:
            v2 = document.get(v2)
        elif vt2 == VALUE_TYPE_OPERATOR:
            v2 = v2.eval(document)

        if isNumberType(v1) and isNumberType(v2):
            return self.compute(float(v1), float(v2))
        elif v1 == None or v2 == None or v1 == undefined or v2 == undefined:
            return None
        else:
            raise self.make_error("%s only supports numeric types" % self.name)

    def compute(self, v1, v2):
        raise NotImplementedError()


class ProjectAddOperator(ProjectDualNumberOperator):

    name = "$add"
    returnTypes = NumberTypes

    def compute(self, v1, v2):
        return v1 + v2


class ProjectSubtractOperator(ProjectDualNumberOperator):

    name = "$subtract"
    returnTypes = NumberTypes

    def compute(self, v1, v2):
        return v1 - v2


class ProjectMultiplyOperator(ProjectDualNumberOperator):

    name = "$multiply"
    returnTypes = NumberTypes

    def compute(self, v1, v2):
        return v1 * v2


class ProjectDivideOperator(ProjectDualNumberOperator):

    name = "$divide"
    returnTypes = NumberTypes

    def compute(self, v1, v2):
        return v1 / v2


class ProjectModOperator(ProjectDualNumberOperator):

    name = "$mod"
    returnTypes = NumberTypes

    def compute(self, v1, v2):
        return v1 % v2



class ProjectConvertOperator(ProjectOperator):

    def __init__(self, key, value, expr=False):
        super(ProjectConvertOperator, self).__init__(key, value, expr=expr)
        if Value.is_doc_ref_key(value):
            self.value = value[1:]
            self.value_type = VALUE_TYPE_REFKEY
        else:
            if isinstance(value, dict):
                prj = OperatorFactory.new_project(key, value, expr=True)
                if (not prj.returnTypes) or (set(prj.returnTypes) & set(types.StringTypes)):
                    self.value = prj
                    self.value_type = VALUE_TYPE_OPERATOR
                else:
                    raise self.make_error("%s source must be string type" % self.name)
            elif not isinstance(value, basestring):
                raise self.make_error("%s source must be string type" % self.name)

    def eval (self, document):
        v = self.value
        if self.value_type == VALUE_TYPE_REFKEY:
            v = document.get(v)
        elif self.value_type == VALUE_TYPE_OPERATOR:
            v = v.eval(document)
        return self.convert(v)

    def convert(self, v):
        raise NotImplementedError()


class ProjectToLowerOperator(ProjectConvertOperator):

    name = "$toLower"
    returnTypes = types.StringTypes

    def convert(self, v):
        if v == undefined or v == None:
            return ""
        return string.lower(v)


class ProjectToUpperOperator(ProjectConvertOperator):

    name = "$toUpper"
    returnTypes = types.StringTypes

    def convert(self, v):
        if v == undefined or v == None:
            return ""
        return string.upper(v)


class ProjectToNumberOperator(ProjectConvertOperator):

    name = "$toNumber"
    returnTypes = NumberTypes

    def convert(self, v):
        if v == undefined or v == None:
            return 0
        return float(v)


class ProjectUseOperator(ProjectOperator):

    name = "$use"
    returnTypes = None

    def __init__(self, key, value, expr=False):
        super(ProjectUseOperator, self).__init__(key, value, expr=expr)
        if isinstance(value, list) and len(value) == 2 or len(value) == 3:
            if Value.is_doc_ref_key(value[0]):
                self.value[0] = value[0][1:]
                self.value_type = VALUE_TYPE_REFKEY
            else:
                if isinstance(value[0], dict):
                    self.value[0] = OperatorFactory.new_project(key, value[0], expr=True)
                    self.value_type = VALUE_TYPE_OPERATOR
            if isinstance(self.value[1], basestring):
                try:
                    self.value[1] = Parser.get(self.value[1])
                except Exception:
                    raise self.make_error("$use does not has predefined parser:%s" % self.value[1])
            elif not callable(self.value[1]):
                raise self.make_error("$use parser must be a callable or predefined")

            if len(value) == 3 and not isinstance(value[2], dict):
                raise self.make_error("$use parser parameter should be dict")
        else:
            raise self.make_error("the $use operator requires an array of two or three elements")

    def eval(self, document):
        v = self.value[0]
        if self.value_type == VALUE_TYPE_REFKEY:
            v = document.get(v)
        elif self.value_type == VALUE_TYPE_OPERATOR:
            v = v.eval(document)

        if len(self.value) == 3:
            return self.value[1](v, **self.value[2])
        else:
            return self.value[1](v)


class ProjectConcatOperator(ProjectOperator):

    name = "$concat"
    returnTypes = types.StringTypes

    def __init__(self, key, value, expr=False):
        super(ProjectConcatOperator, self).__init__(key, value, expr=expr)
        if isinstance(value, list) and len(value) > 2:
            for i, v in enumerate(value):
                if isinstance(v, dict):
                    prj = OperatorFactory.new_project(key, v, expr=True)
                    if (not prj.returnTypes) or (set(prj.returnTypes) & set(types.StringTypes)):
                        self.value[i] = prj
                    else:
                        raise self.make_error("$concat source must be string type")
                elif not isinstance(value[i], basestring):
                    raise self.make_error("$concat source must be string type")
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


class ProjectSubstrOperator(ProjectOperator):

    name = "$substr"
    returnTypes = types.StringTypes

    def __init__(self, key, value, expr=False):
        super(ProjectSubstrOperator, self).__init__(key, value, expr=expr)
        if isinstance(value, list) and len(value) == 3:
            if Value.is_doc_ref_key(value[0]):
                self.value[0] = value[0][1:]
                self.value_type = VALUE_TYPE_REFKEY
            else:
                if isinstance(value[0], dict):
                    prj = OperatorFactory.new_project(key, value[0], expr=True)
                    if (not prj.returnTypes) or (set(prj.returnTypes) & set(types.StringTypes)):
                        self.value[0] = prj
                        self.value_type = VALUE_TYPE_OPERATOR
                    else:
                        raise self.make_error("$substr source must be string type")
                elif not isinstance(value[0], basestring):
                    raise self.make_error("$substr source must be string type")

            if not isNumberType(value[1]):
                raise self.make_error("$substr start pos must be number type")
            self.value[1] = int(value[1])

            if not isNumberType(value[2]) or int(value[2]) < 0:
                raise self.make_error("$substr number chars must be number type")
            self.value[2] = self.value[1] + int(value[2])
        else:
            raise self.make_error("the $substr operator requires an array of three elements")

    def eval(self, document):
        v = self.value[0]
        if self.value_type == VALUE_TYPE_REFKEY:
            v = document.get(v)
        elif self.value_type == VALUE_TYPE_OPERATOR:
            v = v.eval(document)

        if v == undefined:
            return ""
        elif not isinstance(v, basestring):
            raise self.make_error("$substr source must be string type")
        else:
            return v[self.value[1]:self.value[2]]


class ProjectSubstringOperator(ProjectOperator):

    name = "$substring"
    returnTypes = types.StringTypes

    def __init__(self, key, value, expr=False):
        super(ProjectSubstringOperator, self).__init__(key, value, expr=expr)
        if isinstance(value, list) and (len(value) == 2 or len(value) == 3):
            if Value.is_doc_ref_key(value[0]):
                self.value[0] = value[0][1:]
                self.value_type = VALUE_TYPE_REFKEY
            else:
                if isinstance(value[0], dict):
                    prj = OperatorFactory.new_project(key, value[0], expr=True)
                    if (not prj.returnTypes) or (set(prj.returnTypes) & set(types.StringTypes)):
                        self.value[0] = prj
                        self.value_type = VALUE_TYPE_OPERATOR
                    else:
                        raise self.make_error("$substring source must be string type")
                elif not isinstance(value[0], basestring):
                    raise self.make_error("$substring source must be string type")

            if not isNumberType(value[1]):
                raise self.make_error("$substring start pos must be number type")
            self.value[1] = int(value[1])

            if len(value) == 3:
                if not isNumberType(value[2]):
                    raise self.make_error("$substring end pos must be number type")
                self.value[2] = int(value[2])
            else:
                self.value = list(self.value) + [None]
        else:
            raise self.make_error("the $substring operator requires an array of two or three elements")

    def eval(self, document):
        v = self.value[0]
        if self.value_type == VALUE_TYPE_REFKEY:
            v = document.get(v)
        elif self.value_type == VALUE_TYPE_OPERATOR:
            v = v.eval(document)

        if v == undefined:
            return ""
        elif not isinstance(v, basestring):
            raise self.make_error("$substring source must be string type")
        else:
            return v[self.value[1]:self.value[2]]



class ProjectDateOperator(ProjectOperator):

    def __init__(self, key, value, expr=False):
        super(ProjectDateOperator, self).__init__(key, value, expr=expr)
        if Value.is_doc_ref_key(value):
            self.value = value[1:]
            self.value_type = VALUE_TYPE_REFKEY
        else:
            if isinstance(value, dict):
                prj = OperatorFactory.new_project(key, value, expr=True)
                if (not prj.returnTypes) or (set(prj.returnTypes) & set(DateTypes)):
                    self.value = prj
                    self.value_type = VALUE_TYPE_OPERATOR
                else:
                    raise self.make_error("%s value must be date type" % self.name)
            elif (not isinstance(value, (datetime, date))) and (not isNumberType(value)):
                raise self.make_error("%s value must be date type" % self.name)

    def eval(self, document):
        d = self._make_date(document)
        return self._eval(d)

    def _make_date(self, document):
        v = self.value
        if self.value_type == VALUE_TYPE_REFKEY:
            v = document.get(v)
        elif self.value_type == VALUE_TYPE_OPERATOR:
            v = v.eval(document)

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
    returnTypes = [types.IntType]

    def _eval(self, d):
        dy = datetime(d.year, 1, 1)
        return (d-dy).days


class ProjectDayOfMonthOperator(ProjectDateOperator):

    name = "$dayOfMonth"
    returnTypes = [types.IntType]

    def _eval(self, d):
        return d.day


class ProjectDayOfWeekOperator(ProjectDateOperator):

    name = "$dayOfWeek"
    returnTypes = [types.IntType]

    def _eval(self, d):
        return d.weekday() + 1


class ProjectYearOperator(ProjectDateOperator):

    name = "$year"
    returnTypes = [types.IntType]

    def _eval(self, d):
        return d.year


class ProjectMonthOperator(ProjectDateOperator):

    name = "$month"
    returnTypes = [types.IntType]

    def _eval(self, d):
        return d.month


class ProjectHourOperator(ProjectDateOperator):

    name = "$hour"
    returnTypes = [types.IntType]

    def _eval(self, d):
        return d.hour


class ProjectMinuteOperator(ProjectDateOperator):

    name = "$minute"
    returnTypes = [types.IntType]

    def _eval(self, d):
        return d.minute


class ProjectSecondOperator(ProjectDateOperator):

    name = "$second"
    returnTypes = [types.IntType]

    def _eval(self, d):
        return d.second


class ProjectMillisecondOperator(ProjectDateOperator):

    name = "$millisecond"
    returnTypes = [types.IntType]

    def _eval(self, d):
        return d.microsecond // 1000


class ProjectCallOperator(ProjectOperator):

    name = "$call"
    returnTypes = None

    def __init__(self, key, value, expr=False):
        super(ProjectCallOperator, self).__init__(key, value, expr=expr)
        if not callable(value):
            raise self.make_error("the $call operator requires callable")

    def eval(self, document):
        return self.value(document)


class ProjectCombineOperator(ProjectOperator):

    returnTypes = [types.DictType]

    def __init__(self, key, value, expr=False):
        super(ProjectCombineOperator, self).__init__(key, value, expr=expr)
        combined_ops = []
        for k, v in value.iteritems():
            if "." in k:
                raise CommandError("dotted field names are only allowed at the top level", self.command)
            combined_ops.append((k, OperatorFactory.new_project("%s.%s" %(key, k), v, expr=expr)))
        self.combined_ops = combined_ops

    def eval(self, document):
        pv = {}
        for k, combine_op in self.combined_ops:
            v = combine_op.project(document)
            if v != undefined:
                pv[k] = v
        return pv


class GroupOperator(Operator):

    command = "$group"

    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.value_type = VALUE_TYPE_PLAIN

    def init_val(self):
        return undefined

    def result(self, acc_val):
        if acc_val == undefined:
            return None
        return acc_val

    def group(self, document, acc_val):
        try:
            return self.eval(document, acc_val)
        except Exception, e:
            raise self.make_error("%s runtime error: %s" % (self.name, e.message))

    def eval(self, document, acc_val):
        raise NotImplementedError()


class GroupUnaryOperator(GroupOperator):

    def __init__(self, key, value):
        super(GroupUnaryOperator, self).__init__(key, value)
        if Value.is_doc_ref_key(value):
            self.value = value[1:]
            self.value_type = VALUE_TYPE_REFKEY
        else:
            if isinstance(value, dict):
                self.value = OperatorFactory.new_project(key, value, expr=True)
                self.value_type = VALUE_TYPE_OPERATOR
            elif isinstance(value, ArrayTypes):
                raise self.make_error("aggregating group operators are unary (%s)" % self.name)

    def get_value(self, document, default=None):
        if self.value_type == VALUE_TYPE_REFKEY:
            return document.get(self.value, default)
        elif self.value_type == VALUE_TYPE_OPERATOR:
            v = self.value.eval(document)
            if v == undefined:
                v = default
            return v
        else:
            return self.value


class GroupSumOperator(GroupUnaryOperator):

    name = "$sum"

    def init_val(self):
        return 0

    def result(self, acc_val):
        return acc_val

    def eval(self, document, acc_val):
        value = self.get_value(document, default=0)
        try:
            return acc_val + value
        except Exception:
            return acc_val


class GroupMinOperator(GroupUnaryOperator):

    name = "$min"

    def eval(self, document, acc_val):
        value = self.get_value(document, undefined)
        if acc_val == undefined:
            return value
        elif value < acc_val:
            return value
        else:
            return acc_val


class GroupMaxOperator(GroupUnaryOperator):

    name = "$max"

    def eval(self, document, acc_val):
        value = self.get_value(document, undefined)
        if acc_val == undefined:
            return value
        elif value > acc_val:
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

    def init_val(self):
        return set()

    def result(self, acc_val):
        return list(acc_val)

    def eval(self, document, acc_val):
        value = self.get_value(document, undefined)
        if value != undefined:
            acc_val.add(value)
        return acc_val


class GroupPushOperator(GroupUnaryOperator):

    name = "$push"

    def init_val(self):
        return []

    def result(self, acc_val):
        return acc_val

    def eval(self, document, acc_val):
        value = self.get_value(document, undefined)
        if value != undefined:
            acc_val.append(value)
        return acc_val


class GroupConcatToSetOperator(GroupUnaryOperator):

    name = "$concatToSet"

    def init_val(self):
        return set()

    def result(self, acc_val):
        return list(acc_val)

    def eval(self, document, acc_val):
        value = self.get_value(document, undefined)
        if value != undefined:
            acc_val = acc_val | set(list(value))
        return acc_val


class GroupConcatToListOperator(GroupUnaryOperator):

    name = "$concatToList"

    def init_val(self):
        return []

    def result(self, acc_val):
        return acc_val

    def eval(self, document, acc_val):
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
        combined_ops = []
        for k, v in value.iteritems():
            combined_ops.append((k, OperatorFactory.new_group(k, v)))
        self.combined_ops = combined_ops

    def init_val(self):
        doc = Document()
        for k, combine_op in self.combined_ops:
            doc.set(k, combine_op.init_val())
        return doc

    def result(self, acc_val):
        for k, combine_op in self.combined_ops:
            acc_val.set(k, combine_op.result(acc_val.get(k)))
        return dict(acc_val)

    def eval(self, document, acc_val):
        for k, combine_op in self.combined_ops:
            acc_val.set(k, combine_op.group(document, acc_val.get(k)))
        return acc_val
