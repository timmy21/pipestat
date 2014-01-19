# -*- coding: utf-8 -*-

import unittest
from pipestat.pipeline import PipeCmd, MatchPipeCmd, ProjectPipeCmd
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



if __name__ == '__main__':
    unittest.main()
