Stat dataset via pipeline
=============================================================

**pipestat** is a library for stat dataset via pipeline,
which use mongo aggregation framework.

Example
-------------------------------------------------------------

Here is a quick example to get a feeling of **pipestat**,
extract fields for event and stat count and elaplse.

.. code:: python

    >>> from pipestat import pipestat
    >>> pipeline = [
    ...    {
    ...        "$match": {
    ...            "_event": {"$regexp": "Collect\s*app:.*timeline.*end.*elapse"},
    ...        },
    ...    },
    ...    {
    ...        "$project": {
    ...            "app": {"$extract": ["$_event", "app:(\w*)"]},
    ...            "action": {"$extract": ["$_event", "(cached|refresh|locked)"]},
    ...            "elapse": {"$extract": ["$_event", "elapse:([\d.]*)"]},
    ...        },
    ...    },
    ...    {
    ...        "$group": {
    ...            "_id": {
    ...                "app": "$app",
    ...                "action": "$action"
    ...            },
    ...            "count": {"$sum": 1},
    ...            "min_elapse": {"$min": "$elapse"},
    ...            "max_elapse": {"$max": "$elapse"},
    ...            "sum_elapse": {"$sum": "$elapse"},
    ...        }
    ...    },
    ...    {
    ...        "$project": {
    ...            "app": "$_id.app",
    ...            "action": "$_id.action",
    ...            "count": "$count",
    ...            "min_elapse": "$min_elapse",
    ...            "max_elapse": "$max_elapse",
    ...            "avg_elapse": {"$divide": ["$sum_elapse", "$count"]},
    ...        },
    ...    },
    ...    {
    ...        "$sort": {
    ...            "app": 1,
    ...            "action": 1,
    ...        }
    ...    },
    ... ]
    >>> dataset = [{"_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... refresh, elapse:0.105722904205"}, ...]
    >>> pipestat(dataset, pipeline)
    [
      {
        "action": "refresh",
        "count": 1546.0,
        "avg_elapse": 0.18074277347920925,
        "min_elapse": 0.028223991394,
        "max_elapse": 1.28353404999
      },
      {
        "action": "locked",
        "count": 49.0,
        "avg_elapse": 0.023240566253672452,
        "min_elapse": 0.00882887840271,
        "max_elapse": 0.0649328231812
      },
      {
        "action": "cached",
        "count": 11257.0,
        "avg_elapse": 0.016989750962513067,
        "min_elapse": 0.00122380256653,
        "max_elapse": 0.452320814133
      }
    ]
