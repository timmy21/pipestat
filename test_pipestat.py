# -*- coding: utf-8 -*-

import unittest
from pipestat.commands import (
    MatchPipeCmd, ProjectPipeCmd, GroupPipeCmd,
    SortCmd, SkipCmd, LimitCmd, UnwindCmd
)


class SortCmdTest(unittest.TestCase):

    def test_sort(self):
        cmd = SortCmd([
            ("app", 1),
            ("elapse", -1),
        ])
        cmd.feed({"app": "app2", "elapse": 3})
        cmd.feed({"app": "app1", "elapse": 1})
        cmd.feed({"app": "app1", "elapse": 4})
        self.assertListEqual(cmd.documents, [
            {"app": "app1", "elapse": 4},
            {"app": "app1", "elapse": 1},
            {"app": "app2", "elapse": 3},
        ])


class SkipCmdTest(unittest.TestCase):

    def test_skip(self):
        cmd = SkipCmd(1)
        cmd.feed({"app": "app2", "elapse": 3})
        cmd.feed({"app": "app1", "elapse": 1})
        cmd.feed({"app": "app1", "elapse": 4})
        self.assertListEqual(cmd.documents, [
            {"app": "app1", "elapse": 1},
            {"app": "app1", "elapse": 4},
        ])


class LimitCmdTest(unittest.TestCase):

    def test_skip(self):
        cmd = LimitCmd(2)
        try:
            cmd.feed({"app": "app2", "elapse": 3})
            cmd.feed({"app": "app1", "elapse": 1})
            cmd.feed({"app": "app1", "elapse": 4})
        except Exception:
            pass
        self.assertListEqual(cmd.documents, [
            {"app": "app2", "elapse": 3},
            {"app": "app1", "elapse": 1},
        ])


class UnwindCmdTest(unittest.TestCase):

    def test_unwind(self):
        cmd = UnwindCmd("$tags")
        cmd.feed({"app": "app2", "tags": ["tag1", "tag2"]})
        self.assertListEqual(cmd.documents, [
            {"app": "app2", "tags": "tag1"},
            {"app": "app2", "tags": "tag2"},
        ])



if __name__ == '__main__':
    unittest.main()
