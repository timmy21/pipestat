# -*- coding: utf-8 -*-
import sys
from os.path import join, dirname, abspath
sys.path.append(join(dirname(abspath(__file__)), "../"))

import json
from pipestat import pipestat


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
]

dataset = [
    {
       "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... refresh, elapse:1.0",
    },
    {
       "_event": "[2014-01-16 16:13:50,000] DEBUG Collect app:app37 timeline end... cached, elapse:0.01",
    },
    {
       "_event": "[2014-01-16 16:13:50,231] DEBUG Collect app:app40 timeline end... refresh, elapse:2.0",
    },
]

print json.dumps(pipestat(dataset, pipeline), indent=4)
