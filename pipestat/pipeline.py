# -*- coding: utf-8 -*-

import re
import sys
import time
import json
import copy
import collections
from pipestat.bsort import insort


class PipeCmdError(Exception):
    """pipe cmd error"""

class PipeCmdDefineError(Exception):
    """pipe cmd define error"""

class PipeCmdExecuteError(Exception):
    """pipe cmd execute error"""

class LimitExceedError(PipeCmdExecuteError):
    """limit cmd passed item exceed specify count"""


class PipeCmd(object):

    def __init__(self, val):
        self.val = val
        self.next = None
        self._data = []

    def feed(self, item):
        if self.next:
            self.next.feed(item)
        else:
            self._data.append(item)

    def result(self):
        if self.next:
            return self.next.result()
        else:
            return self._data

    def _is_item_key(self, k):
        if isinstance(k, basestring) and len(k) > 0 and k[0] == "$":
            return True
        return False

    def _is_operator(self, k):
        if isinstance(k, basestring) and len(k) > 0 and k[0] == "$":
            return True
        return False

    def _get_val(self, item, k, default=None):
        if self._is_item_key(k):
            parts = k[1:].split(".")
            part_item = item
            for part in parts:
                if part in part_item:
                    part_item = part_item[part]
                else:
                    return default
            return part_item
        else:
            return k

    def _set_val(self, item, k, v):
        parts = k.split(".")
        part_item = item
        for part in parts[:-1]:
            if part not in part_item:
                part_item[part] = {}
            part_item = part_item[part]
        part_item[parts[-1]] = v


class MatchPipeCmd(PipeCmd):

    def feed(self, item):
        matched = True
        for k, ops in self.val.iteritems():
            if not self._is_match(item, "$"+k, ops):
                matched = False
                break

        if matched:
            super(MatchPipeCmd, self).feed(item)

    def _is_match(self, item, k, ops):
        if not isinstance(ops, dict):
            if not getattr(self, "do_eq")(item, k, ops):
                return False
        else:
            for op_k, op_v in ops.iteritems():
                if self._is_operator(op_k):
                    if hasattr(self, "do_%s" % op_k[1:]):
                        if not getattr(self, "do_%s" % op_k[1:])(item, k, op_v):
                            return False
                    else:
                        raise PipeCmdDefineError("$match command do not support %s operator." % op_k)
        return True

    def do_call(self, item, k, op_v):
        if callable(op_v):
            return op_v(self._get_val(item, k), item)
        else:
            raise PipeCmdDefineError("$match command's $call operator value:%r is not be callable." % op_v)

    def do_regexp(self, item, k, op_v):
        v = self._get_val(item, k, default="")
        m = re.search(op_v, v)
        if m:
            return True
        return False

    def do_and(self, item, k, op_v):
        for sub_ops in op_v:
            if not self._is_match(item, k, sub_ops):
                return False
        return True

    def do_or(self, item, k, op_v):
        for sub_ops in op_v:
            if self._is_match(item, k, sub_ops):
                return True
        return False

    def do_gt(self, item, k, op_v):
        v = float(self._get_val(item, k))
        op_v = float(self._get_val(item, op_v))
        return v > op_v

    def do_gte(self, item, k, op_v):
        v = float(self._get_val(item, k))
        op_v = float(self._get_val(item, op_v))
        return v >= op_v

    def do_lt(self, item, k, op_v):
        v = float(self._get_val(item, k))
        op_v = float(self._get_val(item, op_v))
        return v < op_v

    def do_lte(self, item, k, op_v):
        v = float(self._get_val(item, k))
        op_v = float(self._get_val(item, op_v))
        return v <= op_v

    def do_ne(self, item, k, op_v):
        v = self._get_val(item, k)
        op_v = self._get_val(item, op_v)
        return v != op_v

    def do_eq(self, item, k, op_v):
        v = self._get_val(item, k)
        op_v = self._get_val(item, op_v)
        return v == op_v


class ProjectPipeCmd(PipeCmd):

    def feed(self, item):
        new_item = self._new_vals(item, self.val)
        super(ProjectPipeCmd, self).feed(new_item)

    def _new_vals(self, item, val):
        new_vals = {}
        for k, v in val.iteritems():
            if isinstance(v, basestring):
                new_vals[k] = self._get_val(item, v)
            else:
                op_k = v.keys()[0]
                if op_k[0] == "$":
                    new_vals[k] = getattr(self, "do_%s" % op_k[1:])(item, k, v[op_k])
                else:
                    new_vals[k] = self._new_vals(item, v)
        return new_vals

    def do_extract(self, item, k, op_v):
        op_args_k = op_v[0]
        op_args_expr = op_v[1]
        if self._is_item_key(op_args_k):
            v = self._get_val(item, op_args_k, default="")
            m = re.search(op_args_expr, v)
            if m:
                if k in m.groupdict():
                    return m.groupdict()[k]
                elif len(m.groups()) > 0:
                    return m.group(1)
                else:
                    return m.group()
        else:
            raise PipeCmdDefineError("$project command's $extract operator first argument should be item's key, but %r is not start with '$'" % op_args_k)

    def do_timestamp(self, item, k, op_v):
        op_args_k = op_v[0]
        op_args_fmt = op_v[1]
        if self._is_item_key(op_args_k):
            v = self._get_val(item, op_args_k, default="")
            return time.mktime(time.strptime(v, op_args_fmt))
        else:
            raise PipeCmdDefineError("$project command's $timestamp operator first argument should be item's key, but '%s' not start with '$'" % op_args_k)

    def do_call(self, item, k, op_v):
        return op_v(item)
        if callable(op_v):
            return op_v(item)
        else:
            raise PipeCmdDefineError("$project command's $call operator value:%r is not be callable." % op_v)

    def do_add(self, item, k, op_v):
        v1 = float(self._get_val(item, op_v[0]))
        v2 = float(self._get_val(item, op_v[1]))
        return v1+v2

    def do_substract(self, item, k, op_v):
        v1 = float(self._get_val(item, op_v[0]))
        v2 = float(self._get_val(item, op_v[1]))
        return v1-v2

    def do_multiply(self, item, k, op_v):
        v1 = float(self._get_val(item, op_v[0]))
        v2 = float(self._get_val(item, op_v[1]))
        return v1*v2

    def do_divide(self, item, k, op_v):
        v1 = float(self._get_val(item, op_v[0]))
        v2 = float(self._get_val(item, op_v[1]))
        return v1/v2


class GroupPipeCmd(PipeCmd):

    def __init__(self, val):
        super(GroupPipeCmd, self).__init__(val)
        self._gdata = {}

    def feed(self, item):
        ids = self._get_id(item, self.val["_id"])
        gid = json.dumps({"_id": ids})

        new_vals = {}
        old_vals = self._gdata.get(gid, {})
        for k, v in self.val.iteritems():
            if k == "_id":
                continue
            op_k = v.keys()[0]
            if self._is_operator(op_k):
                if hasattr(self, "do_%s" % op_k[1:]):
                    new_vals[k] = getattr(self, "do_%s" % op_k[1:])(item, old_vals.get(k), v[op_k])
                else:
                    raise PipeCmdDefineError("$group command do not support %s operator." % op_k)
            else:
                raise PipeCmdDefineError("$group command has invalid operator:%r" % op_k)

        self._gdata[gid] = new_vals

    def result(self):
        rets = self._make_result()
        if self.next:
            try:
                for item in rets:
                    self.next.feed(item)
            except LimitExceedError:
                pass
            return self.next.result()
        else:
            return rets

    def _make_result(self):
        rets = []
        for k, v in self._gdata.iteritems():
            k = json.loads(k)
            rets.append(dict(k, **v))
        return rets

    def _get_id(self, item, id_v):
        if isinstance(id_v, basestring):
            if id_v[0] == "$":
                return item.get(id_v[1:])
            else:
                return id_v
        elif isinstance(id_v, dict):
            ids = {}
            for k, v in id_v.iteritems():
                ids[k] = self._get_id(item, v)
            return ids
        elif isinstance(id_v, collections.Iterable):
            ids = []
            for v in id_v:
                ids.append(self._get_id(item, v))
            return ids
        else:
            return id_v

    def do_sum(self, item, old_v, op_v):
        op_v = float(self._get_val(item, op_v))
        if old_v is None:
            return op_v
        else:
            return old_v + op_v

    def do_min(self, item, old_v, op_v):
        op_v = float(self._get_val(item, op_v))
        if old_v is None:
            return op_v
        else:
            return min(old_v, op_v)

    def do_max(self, item, old_v, op_v):
        op_v = float(self._get_val(item, op_v))
        if old_v is None:
            return op_v
        else:
            return max(old_v, op_v)

    def do_first(self, item, old_v, op_v):
        if old_v is not None:
            return old_v
        else:
            return self._get_val(item, op_v)

    def do_last(self, item, old_v, op_v):
        op_v = self._get_val(item, op_v)
        if op_v is not None:
            return op_v
        else:
            return old_v

    def do_addToSet(self, item, old_v, op_v):
        op_v = self._get_val(item, op_v)
        if old_v is None:
            if op_v is not None:
                return [op_v]
        else:
            if op_v is not None:
                return list(set(old_v + [op_v]))
            else:
                return old_v

    def do_push(self, item, old_v, op_v):
        op_v = self._get_val(item, op_v)
        if old_v is None:
            if op_v is not None:
                return [op_v]
        else:
            if op_v is not None:
                return old_v + [op_v]
            else:
                return old_v


class SortCmd(PipeCmd):

    def feed(self, item):
        insort(self._data, item, cmp=self.cmp_func)

    def cmp_func(self, item1, item2):
        for k, direction in self.val:
            v1 = self._get_val(item1, "$"+k)
            v2 = self._get_val(item2, "$"+k)
            ret = 0
            if direction == 1:
                ret = cmp(v1, v2)
            elif direction == -1:
                ret = cmp(v2, v1)
            else:
                raise PipeCmdDefineError('Unknow sort direction val:"%s", valid value is 1 or -1.', direction)
            if ret == 0:
                continue
            else:
                return ret
        return 0

    def result(self):
        if self.next:
            try:
                for item in self._data:
                    self.next.feed(item)
            except LimitExceedError:
                pass
            return self.next.result()
        else:
            return self._data


class SkipCmd(PipeCmd):

    def __init__(self, val):
        super(SkipCmd, self).__init__(val)
        self.val = int(val)
        self._skiped = 0

    def feed(self, item):
        if self._skiped >= self.val:
            super(SkipCmd, self).feed(item)
        else:
            self._skiped += 1


class LimitCmd(PipeCmd):

    def __init__(self, val):
        super(LimitCmd, self).__init__(val)
        self.val = int(val)
        self._passed = 0

    def feed(self, item):
        if self._passed < self.val:
            self._passed += 1
            super(LimitCmd, self).feed(item)
        else:
            raise LimitExceedError


class UnwindCmd(PipeCmd):

    def __init__(self, val):
        super(UnwindCmd, self).__init__(val)
        if not self._is_item_key(self.val):
            raise PipeCmdDefineError("$unwind command paramter should be item key, but %r do not start with '$'" % self.val)

    def feed(self, item):
        vals = self._get_val(item, self.val)
        if isinstance(vals, collections.Iterable):
            for v in vals:
                new_item = copy.deepcopy(item)
                self._set_val(new_item, self.val[1:], v)
                super(UnwindCmd, self).feed(new_item)
        else:
            raise PipeCmdExecuteError("$unwind command paramter field's value:%s is not Iterable" % vals)


class Pipeline(object):

    def __init__(self, pipeline):
        self.cmd = None
        prev_cmd = None
        for p in pipeline:
            cmd_k = p.keys()[0]
            cmd_v = p[cmd_k]
            try:
                cmd = self._new_cmd(cmd_k, cmd_v)
            except Exception:
                exc_info = sys.exc_info()
                raise PipeCmdDefineError, exc_info[1], exc_info[2]

            if not cmd:
                raise PipeCmdDefineError('Cannot find command:"%s"' % cmd_k)
            if prev_cmd:
                prev_cmd.next = cmd
            else:
                self.cmd = cmd
            prev_cmd = cmd

        if not self.cmd:
            raise PipeCmdDefineError('Cannot find any command')

    def _new_cmd(self, cmd_k, cmd_v):
        if cmd_k == "$match":
            return MatchPipeCmd(cmd_v)
        elif cmd_k == "$project":
            return ProjectPipeCmd(cmd_v)
        elif cmd_k == "$group":
            return GroupPipeCmd(cmd_v)
        elif cmd_k == "$sort":
            return SortCmd(cmd_v)
        elif cmd_k == "$skip":
            return SkipCmd(cmd_v)
        elif cmd_k == '$limit':
            return LimitCmd(cmd_v)
        elif cmd_k == "$unwind":
            return UnwindCmd(cmd_v)

    def feed(self, item):
        try:
            self.cmd.feed(item)
        except LimitExceedError:
            raise
        except Exception:
            exc_info = sys.exc_info()
            raise PipeCmdExecuteError, exc_info[1], exc_info[2]

    def result(self):
        return self.cmd.result()
