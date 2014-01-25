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
           "elapse": {"$extract": ["$_event", "elapse:([\d.]*)"]},
       },
   },
   {
       "$group": {
           "_id": {
               "app": "$app",
               "action": "$action"
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

dataset = [
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

print json.dumps(pipestat(dataset, pipeline), indent=4)
