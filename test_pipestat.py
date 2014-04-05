# -*- coding: utf-8 -*-

import unittest
import datetime
from pipestat.models import Document, undefined
from pipestat.commands import (
    MatchCommand, ProjectCommand, GroupCommand,
    SortCommand, SkipCommand, LimitCommand, UnwindCommand
)
from pipestat import pipestat


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
            "$call": lambda doc: doc["elapse"]%2 == 0,
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
            Document({"app": "app2", "time": 3}),
            Document({"app": "app1", "time": 1}),
            Document({"app": "app1", "time": 4}),
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

    def test_mod(self):
        cmd = ProjectCommand({
            "elapse": {"$mod": ["$elapse", 2]},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": 1}),
            Document({"elapse": 1}),
            Document({"elapse": 0}),
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

    def test_toNumber(self):
        cmd = ProjectCommand({
            "elapse": {"$toNumber": "$elapse"},
        })
        cmd.feed(Document({"app": "app2", "elapse": "3"}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": 3}),
        ])

    def test_concat(self):
        cmd = ProjectCommand({
            "app": {"$concat": ["$app", "-", {"$toUpper": "$action"}]},
        })
        cmd.feed(Document({"app": "app2", "action": "cached"}))
        self.assertListEqual(cmd.result(), [
            Document({"app": "app2-CACHED"}),
        ])

    def test_dayOfYear(self):
        cmd = ProjectCommand({
            "dy": {"$dayOfYear": "$ts"}
        })
        cmd.feed(Document({"ts": datetime.datetime(2014, 3, 31, 12, 30, 0)}))
        self.assertListEqual(cmd.result(), [
            {"dy": 89}
        ])

    def test_dayOfMonth(self):
        cmd = ProjectCommand({
            "dm": {"$dayOfMonth": "$ts"}
        })
        cmd.feed(Document({"ts": datetime.datetime(2014, 3, 31, 12, 30, 0)}))
        self.assertListEqual(cmd.result(), [
            {"dm": 31}
        ])

    def test_dayOfWeek(self):
        cmd = ProjectCommand({
            "dw": {"$dayOfWeek": "$ts"}
        })
        cmd.feed(Document({"ts": datetime.datetime(2014, 3, 31, 12, 30, 0)}))
        self.assertListEqual(cmd.result(), [
            {"dw": 1}
        ])

    def test_year(self):
        cmd = ProjectCommand({
            "y": {"$year": "$ts"}
        })
        cmd.feed(Document({"ts": datetime.datetime(2014, 3, 31, 12, 30, 0)}))
        self.assertListEqual(cmd.result(), [
            {"y": 2014}
        ])

    def test_month(self):
        cmd = ProjectCommand({
            "m": {"$month": "$ts"}
        })
        cmd.feed(Document({"ts": datetime.datetime(2014, 3, 31, 12, 30, 0)}))
        self.assertListEqual(cmd.result(), [
            {"m": 3}
        ])

    def test_hour(self):
        cmd = ProjectCommand({
            "h": {"$hour": "$ts"}
        })
        cmd.feed(Document({"ts": datetime.datetime(2014, 3, 31, 12, 30, 0)}))
        self.assertListEqual(cmd.result(), [
            {"h": 12}
        ])

    def test_minute(self):
        cmd = ProjectCommand({
            "m": {"$minute": "$ts"}
        })
        cmd.feed(Document({"ts": datetime.datetime(2014, 3, 31, 12, 30, 0)}))
        self.assertListEqual(cmd.result(), [
            {"m": 30}
        ])

    def test_second(self):
        cmd = ProjectCommand({
            "s": {"$second": "$ts"}
        })
        cmd.feed(Document({"ts": datetime.datetime(2014, 3, 31, 12, 30, 0)}))
        self.assertListEqual(cmd.result(), [
            {"s": 0}
        ])

    def test_millisecond(self):
        cmd = ProjectCommand({
            "m": {"$millisecond": "$ts"}
        })
        cmd.feed(Document({"ts": datetime.datetime(2014, 3, 31, 12, 30, 0)}))
        self.assertListEqual(cmd.result(), [
            {"m": 0}
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

    def test_nest_project(self):
        cmd = ProjectCommand({
            "elapse": {"$multiply": [{"$add": ["$elapse", 3]}, 2]},
        })
        cmd.feed(Document({"app": "app2", "elapse": 3}))
        cmd.feed(Document({"app": "app1", "elapse": 1}))
        cmd.feed(Document({"app": "app1", "elapse": 4}))
        self.assertListEqual(cmd.result(), [
            Document({"elapse": 12}),
            Document({"elapse": 8}),
            Document({"elapse": 14}),
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

    def test_call(self):
        def nsum(document, acc_val):
            if acc_val == undefined:
                acc_val = 0
            v = float(document.get("elapse"))
            return v + acc_val

        cmd = GroupCommand({
            "_id": "$app",
            "elapse": {"$call": nsum}
        })
        cmd.feed(Document({"app": "app2", "elapse": "1"}))
        cmd.feed(Document({"app": "app2", "elapse": "4"}))
        cmd.feed(Document({"app": "app1", "elapse": "3"}))
        self.assertListEqual(cmd.result(), [
            {"_id": "app2", "elapse": 5},
            {"_id": "app1", "elapse": 3},
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



class ExamplesTest(unittest.TestCase):

    def setUp(self):
        self.dataset = [
            {
               "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... refresh, elapse:1.0",
            },
            {
               "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... refresh, elapse:2.0",
            },
            {
               "_event": "[2014-01-16 16:13:50,000] DEBUG Collect app:app37 timeline end... cached, elapse:0.01",
            },
            {
               "_event": "[2014-01-16 16:13:50,231] DEBUG Collect app:app40 timeline end... refresh, elapse:2.0",
            },
        ]

    def test1(self):
        pipeline = [
           {
               "$match": {
                   "_event": {"$regexp": "Collect\s*app:.*timeline.*end.*elapse"},
               },
           },
           {
               "$project": {
                   "app": {"$extract": ["$_event", "app:(\w*)"]},
                   "action": {
                       "$extract": ["$_event", "(cached|refresh|locked)"]
                    },
                    "elapse": {
                       "$toNumber": {
                           "$extract": ["$_event", "elapse:([\d.]*)"],
                        },
                    }
               },
           },
           {
               "$group": {
                   "_id": {
                       "app": "$app",
                        "action": {
                            "$toUpper": "$action"
                        }
                   },
                   "count": {"$sum": 1},
                   "min_elapse": {"$min": "$elapse"},
                   "max_elapse": {"$max": "$elapse"},
                   "sum_elapse": {"$sum": "$elapse"},
               }
           },
           {
               "$project": {
                   "app": "$_id.app",
                   "action": "$_id.action",
                   "count": "$count",
                   "elapse": {
                       "min": "$min_elapse",
                       "max": "$max_elapse",
                       "avg": {"$divide": ["$sum_elapse", "$count"]},
                   },
               },
           },
           {
               "$sort": [
                   ("app", 1),
                   ("action", 1),
               ]
           },
        ]
        results = pipestat(self.dataset, pipeline)
        self.assertEqual(results, [
            {
                "app": "app37",
                "action": "CACHED",
                "count": 1,
                "elapse": {
                    "min": 0.01,
                    "max": 0.01,
                    "avg": 0.01,
                }
            },
            {
                "app": "app37",
                "action": "REFRESH",
                "count": 2,
                "elapse": {
                    "min": 1.0,
                    "max": 2.0,
                    "avg": 1.5,
                }
            },
            {
                "app": "app40",
                "action": "REFRESH",
                "count": 1,
                "elapse": {
                    "min": 2.0,
                    "max": 2.0,
                    "avg": 2.0,
                }
            },
        ])

    def test2(self):
        pipeline = [
            {
                "$match": {
                    "_event": {"$regexp": "Collect\s*app:.*timeline.*end.*elapse"},
                },
            },
            {
                "$project": {
                    "app": {"$extract": ["$_event", "app:(\w*)"]},
                    "action": {"$extract": ["$_event", "(cached|refresh|locked)"]},
                },
            },
            {
                "$group": {
                    "_id": {
                        "app": "$app",
                    },
                    "actions": {"$addToSet": {"$toUpper": "$action"}},
                }
            },
            {
                "$project": {
                    "app": "$_id.app",
                    "actions": "$actions",
                },
            },
            {
                "$sort": [
                    ("app", 1),
                ]
            },
        ]
        results = pipestat(self.dataset, pipeline)
        self.assertEqual(results, [
            {"app": "app37", "actions": ["REFRESH", "CACHED"]},
            {"app": "app40", "actions": ["REFRESH"]},
        ])


if __name__ == '__main__':
    unittest.main()
