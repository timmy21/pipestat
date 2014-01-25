# -*- coding: utf-8 -*-

import sys
from pipestat.commands import (
    MatchPipeCmd, ProjectPipeCmd, GroupPipeCmd,
    SortCmd, SkipCmd, LimitCmd, UnwindCmd
)
from pipestat.errors import (
    PipeStatError, PipeCmdError, LimitCmdCompleted
)
from pipestat.models import Document


class Pipeline(object):

    def __init__(self, pipeline):
        self.cmd = None
        prev_cmd = None
        for p in pipeline:
            if not self._valid(p):
                raise PipeCmdError('Invalid pipeline command "%s"' % p)

            cmd_k = p.keys()[0]
            cmd_v = p[cmd_k]
            try:
                cmd = self._new_cmd(cmd_k, cmd_v)
            except PipeStatError:
                raise
            except Exception:
                exc_info = sys.exc_info()
                raise PipeCmdError, exc_info[1], exc_info[2]

            if not cmd:
                raise PipeCmdError('pipeline not support command "%s"' % cmd_k)
            if prev_cmd:
                prev_cmd.next = cmd
            else:
                self.cmd = cmd
            prev_cmd = cmd

        if not self.cmd:
            raise PipeCmdError('pipeline not have any command')

    def _valid(self, pipe):
        if not isinstance(pipe, dict):
            return False
        if len(pipe) != 1:
            return False

        cmd_k = pipe.keys()[0]
        if not cmd_k:
            return False
        if cmd_k[0] != "$":
            return False
        return True

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
            self.cmd.feed(Document(item))
        except LimitCmdCompleted:
            raise
        except PipeStatError:
            raise
        except Exception:
            exc_info = sys.exc_info()
            raise PipeStatError, exc_info[1], exc_info[2]

    def result(self):
        return self.cmd.result()
