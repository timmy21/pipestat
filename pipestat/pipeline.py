# -*- coding: utf-8 -*-

import sys
from pipestat.commands import (
    MatchCommand, ProjectCommand, GroupCommand,
    SortCommand, SkipCommand, LimitCommand, UnwindCommand
)
from pipestat.errors import (
    PipeStatError, CommandError, LimitCompleted
)
from pipestat.models import Document


class Pipeline(object):

    def __init__(self, pipeline):
        self.cmd = None
        prev_cmd = None
        for p in pipeline:
            if not self._valid(p):
                raise CommandError('invalid pipeline command %r' % p)

            cmd_k = p.keys()[0]
            cmd_v = p[cmd_k]
            try:
                cmd = self._new_cmd(cmd_k, cmd_v)
            except PipeStatError:
                raise
            except Exception:
                exc_info = sys.exc_info()
                raise CommandError, exc_info[1], exc_info[2]

            if not cmd:
                raise CommandError('pipeline not support command %r' % p)
            if prev_cmd:
                prev_cmd.next = cmd
            else:
                self.cmd = cmd
            prev_cmd = cmd

        if not self.cmd:
            raise CommandError('pipeline not have any command')

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
            return MatchCommand(cmd_v)
        elif cmd_k == "$project":
            return ProjectCommand(cmd_v)
        elif cmd_k == "$group":
            return GroupCommand(cmd_v)
        elif cmd_k == "$sort":
            return SortCommand(cmd_v)
        elif cmd_k == "$skip":
            return SkipCommand(cmd_v)
        elif cmd_k == '$limit':
            return LimitCommand(cmd_v)
        elif cmd_k == "$unwind":
            return UnwindCommand(cmd_v)

    def feed(self, item):
        try:
            self.cmd.feed(Document(item))
        except LimitCompleted:
            raise
        except PipeStatError:
            raise
        except Exception:
            exc_info = sys.exc_info()
            raise PipeStatError, exc_info[1], exc_info[2]

    def result(self):
        return self.cmd.result()
