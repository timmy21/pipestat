# -*- coding: utf-8 -*-

import re
import sys
import time
import json
import copy
import collections
from .bsort import insort


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

    def _get_val(self, item, k, default=None):
        if isinstance(k, basestring) and k[0] == "$":
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
        parts = k[1:].split(".")
        part_item = item
        for part in parts[:-1]:
            if part not in part_item:
                part_item[part] = {}
            part_item = part_item[part]
        part_item[part[-1]] = v


class MatchPipeCmd(PipeCmd):

    def feed(self, item):
        matched = True
        for k, v in self.val.iteritems():
            if not self._is_match(item, k, v):
                matched = False
                break

        if matched:
            super(MatchPipeCmd, self).feed(item)

    def _is_match(self, item, k, mat_vals):
        for mat_k, mat_v in mat_vals.iteritems():
            if mat_k[0] == "$":
                if not getattr(self, "do_%s" % mat_k[1:])(item, "$"+k, mat_v):
                    return False
            else:
                if not getattr(self, "do_eq")(item, "$"+k, mat_v):
                    return False
        return True

    def do_call(self, item, k, mat_v):
        return mat_v(self._get_val(item, k), item)

    def do_regexp(self, item, k, mat_v):
        m = re.search(mat_v, self._get_val(item, k, default=""))
        if m:
            return True
        return False

    def do_and(self, item, k, mat_v):
        for item in mat_v:
            if not self._is_match(item, k, item):
                return False
        return True

    def do_or(self, item, k, mat_v):
        for item in mat_v:
            if self._is_match(item, k, item):
                return True
        return False

    def do_gt(self, item, k, mat_v):
        v = float(self._get_val(item, k))
        mat_v = float(self._get_val(item, mat_v))
        return v > mat_v

    def do_gte(self, item, k, mat_v):
        v = float(self._get_val(item, k))
        mat_v = float(self._get_val(item, mat_v))
        return v >= mat_v

    def do_lt(self, item, k, mat_v):
        v = float(self._get_val(item, k))
        mat_v = float(self._get_val(item, mat_v))
        return v < mat_v

    def do_lte(self, item, k, mat_v):
        v = float(self._get_val(item, k))
        mat_v = float(self._get_val(item, mat_v))
        return v <= mat_v

    def do_ne(self, item, k, mat_v):
        v = self._get_val(item, k)
        mat_v = self._get_val(item, mat_v)
        return v != mat_v

    def do_eq(self, item, k, mat_v):
        v = self._get_val(item, k)
        mat_v = self._get_val(item, mat_v)
        return v == mat_v


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
                prj_k = v.keys()[0]
                if prj_k[0] == "$":
                    new_vals[k] = getattr(self, "do_%s" % prj_k[1:])(item, k, v[prj_k])
                else:
                    new_vals[k] = self._new_vals(item, v)
        return new_vals

    def do_extract(self, item, k, prj_v):
        field = prj_v[0]
        expr = prj_v[1]
        m = re.search(expr, self._get_val(item, field, default=""))
        if m:
            if k in m.groupdict():
                return m.groupdict()[k]
            elif len(m.groups()) > 0:
                return m.group(1)
            else:
                return m.group()
        else:
            return None

    def do_timestamp(self, item, k, prj_v):
        field = prj_v[0]
        fmt = prj_v[1]
        return time.mktime(time.strptime(self._get_val(item, field), fmt))

    def do_call(self, item, k, prj_v):
        return prj_v(item)

    def do_add(self, item, k, prj_v):
        v1 = float(self._get_val(item, prj_v[0]))
        v2 = float(self._get_val(item, prj_v[1]))
        return v1+v2

    def do_substract(self, item, k, prj_v):
        v1 = float(self._get_val(item, prj_v[0]))
        v2 = float(self._get_val(item, prj_v[1]))
        return v1-v2

    def do_multiply(self, item, k, prj_v):
        v1 = float(self._get_val(item, prj_v[0]))
        v2 = float(self._get_val(item, prj_v[1]))
        return v1*v2

    def do_divide(self, item, k, prj_v):
        v1 = float(self._get_val(item, prj_v[0]))
        v2 = float(self._get_val(item, prj_v[1]))
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
            gp_k = v.keys()[0]
            gp_v = v[gp_k]
            new_vals[k] = getattr(self, "do_%s" % gp_k[1:])(item, old_vals.get(k), gp_v)

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

    def do_sum(self, item, old_v, gp_v):
        new_v = float(self._get_val(item, gp_v))
        if old_v is None:
            return new_v
        else:
            return old_v + new_v

    def do_min(self, item, old_v, gp_v):
        new_v = float(self._get_val(item, gp_v))
        if old_v is None:
            return new_v
        else:
            return min(old_v, new_v)

    def do_max(self, item, old_v, gp_v):
        new_v = float(self._get_val(item, gp_v))
        if old_v is None:
            return new_v
        else:
            return max(old_v, new_v)

    def do_first(self, item, old_v, gp_v):
        if old_v is not None:
            return old_v
        else:
            new_v = self._get_val(item, gp_v)
            return new_v

    def do_last(self, item, old_v, gp_v):
        new_v = self._get_val(item, gp_v)
        if new_v is not None:
            return new_v
        else:
            return old_v

    def do_addToSet(self, item, old_v, gp_v):
        new_v = self._get_val(item, gp_v)
        if old_v is None:
            if new_v is not None:
                return [new_v]
        else:
            if new_v is not None:
                return list(set(old_v + [new_v]))
            else:
                return old_v

    def do_push(self, item, old_v, gp_v):
        new_v = self._get_val(item, gp_v)
        if old_v is None:
            if new_v is not None:
                return [new_v]
        else:
            if new_v is not None:
                return old_v + [new_v]
            else:
                return old_v


class SortCmd(PipeCmd):

    def feed(self, item):
        insort(self._data, item, cmp=self.cmp_func)

    def cmp_func(self, item1, item2):
        for k, direction in self.val.iteritems():
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
            self._skiped += 1
            super(SkipCmd, self).feed(item)


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

    def feed(self, item):
        if self.val[0] != "$":
            raise PipeCmdDefineError("Invalid unwind field:%s" % self.val)

        vals = self._get_val(item, self.val)
        if isinstance(vals, collections.Iterable):
            for v in vals:
                new_item = copy.deepcopy(item)
                self._set_val(item, self.val[1:], v)
                super(UnwindCmd, self).feed(new_item)
        else:
            raise PipeCmdExecuteError("unwind field value:%s, is not Iterable" % vals)


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
