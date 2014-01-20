# -*- coding: utf-8 -*-

import unittest
import json
from pipestat.pipeline import (
    PipeCmd, MatchPipeCmd, ProjectPipeCmd, GroupPipeCmd,
    SortCmd, SkipCmd, LimitCmd, UnwindCmd
)
from pipestat.pipeline import PipeCmdDefineError


class PipeCmdTest(unittest.TestCase):

    def test_get_val(self):
        cmd = PipeCmd(None)
        self.assertEqual(cmd._get_val({"app": "app1"}, "$app"), "app1")
        self.assertEqual(cmd._get_val({"app": "app1"}, "$name"), None)
        self.assertEqual(cmd._get_val({"app": "app1"}, "$name", default=""), "")
        self.assertEqual(cmd._get_val({"app": "app1"}, 1), 1)
        self.assertEqual(cmd._get_val({"app": {"name": "app1"}}, "$app.name"), "app1")

    def test_set_val(self):
        cmd = PipeCmd(None)
        item = {}
        cmd._set_val(item, "name", "app1")
        self.assertDictEqual(item, {"name": "app1"})

        item = {}
        cmd._set_val(item, "app.name", "app1")
        self.assertDictEqual(item, {"app": {"name": "app1"}})


class MatchPipeCmdTest(unittest.TestCase):

    def test_call(self):
        cmd = MatchPipeCmd({
            "name": {"$call": lambda x, item: x.startswith("app")}
        })
        cmd.feed({"name": "app1"})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "name": {"$call": lambda x, item: x.startswith("app")}
        })
        cmd.feed({"name": "test"})
        self.assertEqual(len(cmd._data), 0)

        with self.assertRaises(PipeCmdDefineError):
            cmd = MatchPipeCmd({
                "name": {"$call": "un-callable"}
            })
            cmd.feed({"name": "test"})

    def test_regexp(self):
        cmd = MatchPipeCmd({
            "name": {"$regexp": "app\d+"}
        })
        cmd.feed({"name": "app1"})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "name": {"$regexp": "app\d+"}
        })
        cmd.feed({"name": "test"})
        self.assertEqual(len(cmd._data), 0)


class ProjectPipeCmdTest(unittest.TestCase):

    def test_extract(self):
        cmd = ProjectPipeCmd({
            "app": {"$extract": ["$event", "app:(app\d+)"]}
        })
        cmd.feed({"event": "do action for app:app1"})
        self.assertEqual(cmd._data, [{"app": "app1"}])

    def test_timestamp(self):
        cmd = ProjectPipeCmd({
            "ts": {"$timestamp": ["$timestr", "%Y-%m-%d %H:%M:%S"]}
        })
        cmd.feed({"timestr": "2014-01-19 22:00:00"})
        self.assertEqual(cmd._data, [{"ts": 1390140000.0}])

    def test_call(self):
        cmd = ProjectPipeCmd({
            "ts": {"$call": lambda x: x["ts"]//5*5}
        })
        cmd.feed({"ts": 11})
        self.assertEqual(cmd._data, [{"ts": 10}])


class GroupPipeCmdTest(unittest.TestCase):

    def test_sum(self):
        cmd = GroupPipeCmd({
            "_id": "$app",
            "elapse": {"$sum": "$elapse"},
        })
        cmd.feed({"app": "app1", "elapse": 1})
        cmd.feed({"app": "app1", "elapse": 2})
        cmd.feed({"app": "app2", "elapse": 1})
        self.assertEqual(cmd._gdata, {
            json.dumps({"_id": "app1"}): {"elapse": 3},
            json.dumps({"_id": "app2"}): {"elapse": 1},
        })


class SortCmdTest(unittest.TestCase):

    def test_sort(self):
        cmd = SortCmd([
            ("app", 1),
            ("elapse", -1),
        ])
        cmd.feed({"app": "app2", "elapse": 3})
        cmd.feed({"app": "app1", "elapse": 1})
        cmd.feed({"app": "app1", "elapse": 4})
        self.assertListEqual(cmd._data, [
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
        self.assertListEqual(cmd._data, [
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
        self.assertListEqual(cmd._data, [
            {"app": "app2", "elapse": 3},
            {"app": "app1", "elapse": 1},
        ])


class UnwindCmdTest(unittest.TestCase):

    def test_unwind(self):
        cmd = UnwindCmd("$tags")
        cmd.feed({"app": "app2", "tags": ["tag1", "tag2"]})
        self.assertListEqual(cmd._data, [
            {"app": "app2", "tags": "tag1"},
            {"app": "app2", "tags": "tag2"},
        ])



if __name__ == '__main__':
    unittest.main()
