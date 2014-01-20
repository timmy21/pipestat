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

    def test_and(self):
        cmd = MatchPipeCmd({
            "val": {"$and": [{"$gte": 50}, {"$ne": 70}]}
        })
        cmd.feed({"val": 60})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "val": {"$and": [{"$gte": 50}, {"$ne": 70}]}
        })
        cmd.feed({"val": 70})
        self.assertEqual(len(cmd._data), 0)

    def test_or(self):
        cmd = MatchPipeCmd({
            "val": {"$or": [{"$gte": 50}, {"$eq": 10}]}
        })
        cmd.feed({"val": 60})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "val": {"$or": [{"$gte": 50}, {"$eq": 10}]}
        })
        cmd.feed({"val": 20})
        self.assertEqual(len(cmd._data), 0)

        cmd = MatchPipeCmd({
            "val": {"$or": [{"$gte": 50}, {"$eq": 10}]}
        })
        cmd.feed({"val": 10})
        self.assertEqual(len(cmd._data), 1)

    def test_gt(self):
        cmd = MatchPipeCmd({
            "val": {"$gt": 50}
        })
        cmd.feed({"val": 60})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "val": {"$gt": 50}
        })
        cmd.feed({"val": 40})
        self.assertEqual(len(cmd._data), 0)

    def test_gte(self):
        cmd = MatchPipeCmd({
            "val": {"$gte": 50}
        })
        cmd.feed({"val": 50})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "val": {"$gte": 50}
        })
        cmd.feed({"val": 40})
        self.assertEqual(len(cmd._data), 0)

    def test_lt(self):
        cmd = MatchPipeCmd({
            "val": {"$lt": 50}
        })
        cmd.feed({"val": 40})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "val": {"$lt": 50}
        })
        cmd.feed({"val": 60})
        self.assertEqual(len(cmd._data), 0)

    def test_lte(self):
        cmd = MatchPipeCmd({
            "val": {"$lte": 50}
        })
        cmd.feed({"val": 50})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "val": {"$lte": 50}
        })
        cmd.feed({"val": 60})
        self.assertEqual(len(cmd._data), 0)

    def test_ne(self):
        cmd = MatchPipeCmd({
            "val": {"$ne": 50}
        })
        cmd.feed({"val": 51})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "val": {"$ne": 50}
        })
        cmd.feed({"val": 50})
        self.assertEqual(len(cmd._data), 0)

    def test_eq(self):
        cmd = MatchPipeCmd({
            "val": {"$eq": 50}
        })
        cmd.feed({"val": 50})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "val": 50
        })
        cmd.feed({"val": 50})
        self.assertEqual(len(cmd._data), 1)

        cmd = MatchPipeCmd({
            "val": {"$eq": 50}
        })
        cmd.feed({"val": 51})
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

    def test_add(self):
        cmd = ProjectPipeCmd({
            "total": {"$add": ["$val", 10]}
        })
        cmd.feed({"val": 5})
        self.assertEqual(cmd._data, [{"total": 15}])

        cmd = ProjectPipeCmd({
            "total": {"$add": ["$val1", "$val2"]}
        })
        cmd.feed({"val1": 5, "val2": 5})
        self.assertEqual(cmd._data, [{"total": 10}])

    def test_substract(self):
        cmd = ProjectPipeCmd({
            "tail": {"$substract": ["$val", 10]}
        })
        cmd.feed({"val": 5})
        self.assertEqual(cmd._data, [{"tail": -5}])

        cmd = ProjectPipeCmd({
            "tail": {"$substract": ["$val1", "$val2"]}
        })
        cmd.feed({"val1": 5, "val2": 3})
        self.assertEqual(cmd._data, [{"tail": 2}])

    def test_multiply(self):
        cmd = ProjectPipeCmd({
            "total": {"$multiply": ["$val", 10]}
        })
        cmd.feed({"val": 5})
        self.assertEqual(cmd._data, [{"total": 50}])

        cmd = ProjectPipeCmd({
            "total": {"$multiply": ["$val1", "$val2"]}
        })
        cmd.feed({"val1": 5, "val2": 3})
        self.assertEqual(cmd._data, [{"total": 15}])

    def test_divide(self):
        cmd = ProjectPipeCmd({
            "total": {"$divide": ["$val", 10]}
        })
        cmd.feed({"val": 5})
        self.assertEqual(cmd._data, [{"total": 0.5}])

        cmd = ProjectPipeCmd({
            "total": {"$divide": ["$val1", "$val2"]}
        })
        cmd.feed({"val1": 5, "val2": 2})
        self.assertEqual(cmd._data, [{"total": 2.5}])


class GroupPipeCmdTest(unittest.TestCase):

    def test_get_id(self):
        cmd = GroupPipeCmd({
            "_id": "$app",
        })
        ids = cmd._get_id({"app": "app1"}, cmd.val["_id"])
        self.assertEqual(ids, "app1")

        cmd = GroupPipeCmd({
            "_id": {"app": "$app", "action": "$action"},
        })
        ids = cmd._get_id({"app": "app1", "action": "cached"}, cmd.val["_id"])
        self.assertEqual(ids, {"app": "app1", "action": "cached"})

        cmd = GroupPipeCmd({
            "_id": {"app": "$app", "info": {"action": "$action"}},
        })
        ids = cmd._get_id({"app": "app1", "action": "cached"}, cmd.val["_id"])
        self.assertEqual(ids, {"app": "app1", "info": {"action": "cached"}})

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

    def test_min(self):
        cmd = GroupPipeCmd({
            "_id": "$app",
            "min_elapse": {"$min": "$elapse"},
        })
        cmd.feed({"app": "app1", "elapse": 1})
        cmd.feed({"app": "app1", "elapse": 2})
        self.assertEqual(cmd._gdata, {
            json.dumps({"_id": "app1"}): {"min_elapse": 1},
        })

    def test_max(self):
        cmd = GroupPipeCmd({
            "_id": "$app",
            "max_elapse": {"$max": "$elapse"},
        })
        cmd.feed({"app": "app1", "elapse": 1})
        cmd.feed({"app": "app1", "elapse": 2})
        self.assertEqual(cmd._gdata, {
            json.dumps({"_id": "app1"}): {"max_elapse": 2},
        })

    def test_first(self):
        cmd = GroupPipeCmd({
            "_id": "$app",
            "elapse": {"$first": "$elapse"},
        })
        cmd.feed({"app": "app1", "elapse": 2})
        cmd.feed({"app": "app1", "elapse": 1})
        cmd.feed({"app": "app1", "elapse": 5})
        self.assertEqual(cmd._gdata, {
            json.dumps({"_id": "app1"}): {"elapse": 2},
        })

    def test_last(self):
        cmd = GroupPipeCmd({
            "_id": "$app",
            "elapse": {"$last": "$elapse"},
        })
        cmd.feed({"app": "app1", "elapse": 5})
        cmd.feed({"app": "app1", "elapse": 1})
        cmd.feed({"app": "app1", "elapse": 2})
        self.assertEqual(cmd._gdata, {
            json.dumps({"_id": "app1"}): {"elapse": 2},
        })

    def test_addToSet(self):
        cmd = GroupPipeCmd({
            "_id": "$app",
            "actions": {"$addToSet": "$action"},
        })
        cmd.feed({"app": "app1", "action": "refresh"})
        cmd.feed({"app": "app1", "action": "cached"})
        cmd.feed({"app": "app1", "action": "cached"})
        self.assertEqual(cmd._gdata, {
            json.dumps({"_id": "app1"}): {"actions": ["cached", "refresh"]},
        })

    def test_push(self):
        cmd = GroupPipeCmd({
            "_id": "$app",
            "actions": {"$push": "$action"},
        })
        cmd.feed({"app": "app1", "action": "refresh"})
        cmd.feed({"app": "app1", "action": "cached"})
        cmd.feed({"app": "app1", "action": "cached"})
        self.assertEqual(cmd._gdata, {
            json.dumps({"_id": "app1"}): {"actions": ["refresh", "cached", "cached"]},
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
