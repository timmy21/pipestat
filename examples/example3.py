# -*- coding: utf-8 -*-
import sys
from os.path import join, dirname, abspath
sys.path.append(join(dirname(abspath(__file__)), "../"))

import json
from pipestat import Pipeline, LimitCompleted


pipeline1 = Pipeline([
    {
        "$match": {
            "_event": {"$regexp": "Collect\s*app:.*timeline.*end.*elapse"},
        },
    },
    {
        "$project": {
            "app": {"$extract": ["$_event", "app:(\w*)"]},
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
            },
            "count": {"$sum": 1},
            "sum_elapse": {"$sum": "$elapse"},
        }
    },
    {
        "$project": {
            "app": "$_id.app",
            "avg_elapse": {"$divide": ["$sum_elapse", "$count"]},
        },
    },
    {
        "$sort": [
            ("app", 1),
        ]
    },
])

pipeline2 = Pipeline([
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
            "actions": {"$addToSet": "$action"},
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
])

dataset = [
    {
        "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... refresh, elapse:1.0",
    },
    {
        "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... cached, elapse:0.01",
    },
    {
        "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app40 timeline end... refresh, elapse:2.0",
    },
]

pipes = [pipeline1, pipeline2]

for item in dataset:
    for p in pipes:
        try:
            p.feed(item)
        except LimitCompleted:
            pipes.remove(p)

print json.dumps(pipeline1.result(), indent=4)
print json.dumps(pipeline2.result(), indent=4)
