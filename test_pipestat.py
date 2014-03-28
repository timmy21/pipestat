# -*- coding: utf-8 -*-

import unittest
from pipestat.models import Document
from pipestat.commands import (
    MatchCommand, ProjectCommand, GroupCommand,
    SortCommand, SkipCommand, LimitCommand, UnwindCommand
)


class MatchCommandTest(unittest.TestCase):

    def test_regexp(self):
        cmd = MatchCommand({
            "app": {"$regexp": "app\d+"}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "test", "elapse": 5}))
        cmd.feed(Document({"elapse": 5}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2", "elapse": 3}),
        ])

    def test_lt(self):
        cmd = MatchCommand({
            "elapse": {"$lt": 3}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app1", "elapse": 1}),
        ])

    def test_lte(self):
        cmd = MatchCommand({
            "elapse": {"$lte": 3}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2", "elapse": 3}),
            Document({"app": "app1", "elapse": 1}),
        ])

    def test_gt(self):
        cmd = MatchCommand({
            "elapse": {"$gt": 3}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app1", "elapse": 4}),
        ])

    def test_gte(self):
        cmd = MatchCommand({
            "elapse": {"$gte": 3}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2", "elapse": 3}),
            Document({"app": "app1", "elapse": 4}),
        ])

    def test_eq(self):
        cmd = MatchCommand({
            "app": "app2",
            "elapse": {"$eq": 3},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2", "elapse": 3}),
        ])

    def test_ne(self):
        cmd = MatchCommand({
            "elapse": {"$ne": 3},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app1", "elapse": 1}),
            Document({"app": "app1", "elapse": 4}),
        ])

    def test_in(self):
        cmd = MatchCommand({
            "app": {"$in": ["app1", "app3"]},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app1", "elapse": 1}),
            Document({"app": "app1", "elapse": 4}),
        ])

    def test_nin(self):
        cmd = MatchCommand({
            "app": {"$nin": ["app1", "app3"]},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2", "elapse": 3}),
        ])

    def test_call(self):
        cmd = MatchCommand({
            "elapse": {"$call": lambda x, doc: x%2 == 0},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app1", "elapse": 4}),
        ])

    def test_and(self):
        cmd = MatchCommand({
            "$and": [
                {"app": "app1"},
                {"elapse": {"$gte": 1, "$lt": 4}}
            ],
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app1", "elapse": 1}),
        ])

    def test_or(self):
        cmd = MatchCommand({
            "$or": [
                {"app": "app2"},
                {"elapse": {"$gte": 1, "$lt": 4}}
            ],
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2", "elapse": 3}),
            Document({"app": "app1", "elapse": 1}),
        ])

    def test_combine(self):
        cmd = MatchCommand({
            "elapse": {"$gte": 1, "$lt": 4}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2", "elapse": 3}),
            Document({"app": "app1", "elapse": 1}),
        ])


class ProjectCommandTest(unittest.TestCase):

    def test_exclude(self):
        cmd = ProjectCommand({
            "app": 0,
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": 3}),
            Document({"elapse": 1}),
            Document({"elapse": 4}),
        ])

    def test_value(self):
        cmd = ProjectCommand({
            "count": 1,
            "app": 1,
            "time": {"$value": "$elapse"},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"count": None, "app": "app2", "time": 3}),
            Document({"count": None, "app": "app1", "time": 1}),
            Document({"count": None, "app": "app1", "time": 4}),
        ])

    def test_extract(self):
        cmd = ProjectCommand({
            "appid": {"$extract": ["$app", "app(\d+)"]},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"appid": '2'}),
            Document({"appid": '1'}),
            Document({"appid": '1'}),
        ])

    def test_timestamp(self):
        cmd = ProjectCommand({
            "ts": {"$timestamp": ["$time", "%Y-%m-%d %H:%M:%S"]},
        })
        cmd.feed(Document({"app": "app1", "elapse": 4, "time": "2014-01-26 01:00:00"}))
        self.assertListEqual(cmd.result(), [
            Document({"ts": 1390669200.0}),
        ])

    def test_add(self):
        cmd = ProjectCommand({
            "elapse": {"$add": ["$elapse", 10]},
            "compute": {"$add": ["$elapse", "$wait"]},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3, "wait": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 1, "wait": 0.2}))
        cmd.feed(Document({"app": "app1", "elapse": 4, "wait": 1.1}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": 13, "compute": 4}),
            Document({"elapse": 11, "compute": 1.2}),
            Document({"elapse": 14, "compute": 5.1}),
        ])

    def test_substract(self):
        cmd = ProjectCommand({
            "elapse": {"$substract": ["$elapse", 1]},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": 2}),
            Document({"elapse": 0}),
            Document({"elapse": 3}),
        ])

    def test_multiply(self):
        cmd = ProjectCommand({
            "elapse": {"$multiply": ["$elapse", 2]},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": 6}),
            Document({"elapse": 2}),
            Document({"elapse": 8}),
        ])

    def test_divide(self):
        cmd = ProjectCommand({
            "elapse": {"$divide": ["$elapse", 2]},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": 1.5}),
            Document({"elapse": 0.5}),
            Document({"elapse": 2}),
        ])

    def test_toLower(self):
        cmd = ProjectCommand({
            "app": {"$toLower": "$app"},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "App1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2"}),
            Document({"app": "app1"}),
            Document({"app": "app1"}),
        ])

    def test_toUpper(self):
        cmd = ProjectCommand({
            "app": {"$toUpper": "$app"},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "App1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "APP2"}),
            Document({"app": "APP1"}),
            Document({"app": "APP1"}),
        ])

    def test_call(self):
        cmd = ProjectCommand({
            "elapse": {"$call": lambda doc: 2*(doc["elapse"]+2)},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "App1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": 10}),
            Document({"elapse": 6}),
            Document({"elapse": 12}),
        ])

    def test_combine(self):
        cmd = ProjectCommand({
            "elapse": {
                "value": "$elapse",
                "wait": {"$substract": ["$elapse", 0.5]}
            }
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "App1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": {"value": 3, "wait": 2.5}}),
            Document({"elapse": {"value": 1, "wait": 0.5}}),
            Document({"elapse": {"value": 4, "wait": 3.5}}),
        ])


class GroupCommandTest(unittest.TestCase):

    def test_sum(self):
        cmd = GroupCommand({
            "_id": "$app",
            "elapse": {"$sum": "$elapse"}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "elapse": 3},
            {"_id": "app1", "elapse": 5},
        ])

    def test_min(self):
        cmd = GroupCommand({
            "_id": "$app",
            "elapse": {"$min": "$elapse"}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "elapse": 3},
            {"_id": "app1", "elapse": 1},
        ])

    def test_max(self):
        cmd = GroupCommand({
            "_id": "$app",
            "elapse": {"$max": "$elapse"}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "elapse": 3},
            {"_id": "app1", "elapse": 4},
        ])

    def test_first(self):
        cmd = GroupCommand({
            "_id": "$app",
            "elapse": {"$first": "$elapse"}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "elapse": 3},
            {"_id": "app1", "elapse": 1},
        ])

    def test_last(self):
        cmd = GroupCommand({
            "_id": "$app",
            "elapse": {"$last": "$elapse"}
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "elapse": 3},
            {"_id": "app1", "elapse": 4},
        ])

    def test_addToSet(self):
        cmd = GroupCommand({
            "_id": "$app",
            "ips": {"$addToSet": "$ip"}
        })
        cmd.feed(Document({"app": "app2", "ip": "1.1.1.1"}))
        cmd.feed(Document({"app": "app1", "ip": "1.1.1.2"}))
        cmd.feed(Document({"app": "app1", "ip": "1.1.1.3"}))
        cmd.feed(Document({"app": "app1", "ip": "1.1.1.2"}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "ips": ["1.1.1.1"]},
            {"_id": "app1", "ips": ["1.1.1.2", "1.1.1.3"]},
        ])

    def test_push(self):
        cmd = GroupCommand({
            "_id": "$app",
            "ips": {"$push": "$ip"}
        })
        cmd.feed(Document({"app": "app2", "ip": "1.1.1.1"}))
        cmd.feed(Document({"app": "app1", "ip": "1.1.1.2"}))
        cmd.feed(Document({"app": "app1", "ip": "1.1.1.3"}))
        cmd.feed(Document({"app": "app1", "ip": "1.1.1.2"}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "ips": ["1.1.1.1"]},
            {"_id": "app1", "ips": ["1.1.1.2", "1.1.1.3", "1.1.1.2"]},
        ])

    def test_concatToSet(self):
        cmd = GroupCommand({
            "_id": "$app",
            "ips": {"$concatToSet": "$ips"}
        })
        cmd.feed(Document({"app": "app2", "ips": ["1.1.1.1"]}))
        cmd.feed(Document({"app": "app1", "ips": ["1.1.1.2"]}))
        cmd.feed(Document({"app": "app1", "ips": ["1.1.1.3", "1.1.1.2"]}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "ips": ["1.1.1.1"]},
            {"_id": "app1", "ips": ["1.1.1.2", "1.1.1.3"]},
        ])

    def test_concatToList(self):
        cmd = GroupCommand({
            "_id": "$app",
            "ips": {"$concatToList": "$ips"}
        })
        cmd.feed(Document({"app": "app2", "ips": ["1.1.1.1"]}))
        cmd.feed(Document({"app": "app1", "ips": ["1.1.1.2"]}))
        cmd.feed(Document({"app": "app1", "ips": ["1.1.1.3", "1.1.1.2"]}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "ips": ["1.1.1.1"]},
            {"_id": "app1", "ips": ["1.1.1.2", "1.1.1.3", "1.1.1.2"]},
        ])


class SortCommandTest(unittest.TestCase):

    def test_sort(self):
        cmd = SortCommand([
            ("app", 1),
            ("elapse", -1),
        ])
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app1", "elapse": 4}),
            Document({"app": "app1", "elapse": 1}),
            Document({"app": "app2", "elapse": 3}),
        ])

        cmd = SortCommand({"elapse": -1})
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app1", "elapse": 4}),
            Document({"app": "app2", "elapse": 3}),
            Document({"app": "app1", "elapse": 1}),
        ])


class SkipCommandTest(unittest.TestCase):

    def test_skip(self):
        cmd = SkipCommand(1)
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app1", "elapse": 1}),
            Document({"app": "app1", "elapse": 4}),
        ])


class LimitCommandTest(unittest.TestCase):

    def test_limit(self):
        cmd = LimitCommand(2)
        try:
            cmd.feed(Document({"app": "app2", "elapse": 3}))
            cmd.feed(Document({"app": "app1", "elapse": 1}))
            cmd.feed(Document({"app": "app1", "elapse": 4}))
        except Exception:
            pass
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2", "elapse": 3}),
            Document({"app": "app1", "elapse": 1}),
        ])


class UnwindCommandTest(unittest.TestCase):

    def test_unwind(self):
        cmd = UnwindCommand("$tags")
        cmd.feed(Document({"app": "app2", "tags": ["tag1", "tag2"]}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2", "tags": "tag1"}),
            Document({"app": "app2", "tags": "tag2"}),
        ])


if __name__ == '__main__':
    unittest.main()
