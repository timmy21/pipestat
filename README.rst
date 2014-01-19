Stat dataset via pipeline
=============================================================

**pipestat** is a library for stat dataset via pipeline,
which use mongo aggregation framework syntax.
see this `mongo aggregation pipeline
<http://docs.mongodb.org/manual/core/aggregation-pipeline/>`_ for how pipeline work.

Example
-------------------------------------------------------------

Here is a quick example to get a feeling of **pipestat**,
extract fields from event, and then stat count and elapse:

.. code:: python

    >>> from pipestat import pipestat

    >>> pipeline = [
    ...     {
    ...         "$match": {
    ...             "_event": {"$regexp": "Collect\s*app:.*timeline.*end.*elapse"},
    ...         },
    ...     },
    ...     {
    ...         "$project": {
    ...             "app": {"$extract": ["$_event", "app:(\w*)"]},
    ...             "action": {"$extract": ["$_event", "(cached|refresh|locked)"]},
    ...             "elapse": {"$extract": ["$_event", "elapse:([\d.]*)"]},
    ...         },
    ...     },
    ...     {
    ...         "$group": {
    ...             "_id": {
    ...                 "app": "$app",
    ...                 "action": "$action"
    ...             },
    ...             "count": {"$sum": 1},
    ...             "min_elapse": {"$min": "$elapse"},
    ...             "max_elapse": {"$max": "$elapse"},
    ...             "sum_elapse": {"$sum": "$elapse"},
    ...         }
    ...     },
    ...     {
    ...         "$project": {
    ...             "app": "$_id.app",
    ...             "action": "$_id.action",
    ...             "count": "$count",
    ...             "min_elapse": "$min_elapse",
    ...             "max_elapse": "$max_elapse",
    ...             "avg_elapse": {"$divide": ["$sum_elapse", "$count"]},
    ...         },
    ...     },
    ...     {
    ...         "$sort": [
    ...             ("app", 1),
    ...             ("action", 1),
    ...         ]
    ...     },
    ... ]

    >>> dataset = [
    ...     {
    ...         "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... refresh, elapse:1.0",
    ...     },
    ...     {
    ...         "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... cached, elapse:0.01",
    ...     },
    ...     {
    ...         "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app40 timeline end... refresh, elapse:2.0",
    ...     },
    ... ]

    >>> pipestat(dataset, pipeline)
    [
        {
            "count": 1.0,
            "avg_elapse": 0.01,
            "app": "app37",
            "action": "cached",
            "min_elapse": 0.01,
            "max_elapse": 0.01
        },
        {
            "count": 1.0,
            "avg_elapse": 1.0,
            "app": "app37",
            "action": "refresh",
            "min_elapse": 1.0,
            "max_elapse": 1.0
        },
        {
            "count": 1.0,
            "avg_elapse": 2.0,
            "app": "app40",
            "action": "refresh",
            "min_elapse": 2.0,
            "max_elapse": 2.0
        }
    ]

What commands pipestat support
---------------------------------------------------------------------------------

$match
~~~~~~

$match pipes the documents that match its conditions to the next operator in the pipeline.
See this `mongo aggregation $match
<http://docs.mongodb.org/manual/reference/operator/aggregation/match/>`_ for more.

$match command support basic operators: $and, $or, $gt, $gte, $lt, $lte, $ne and $eq,
in addition to this, pipestat $match command support more, like **$regexp**, **$call**.

$regex operator use regular expression to match specify field value, use like below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$match": {
    ...            "_event": {"$regexp": "Collect\s*app:.*timeline.*end.*elapse"},
    ...        },
    ...    },
    ... ]

$call operator use callable which return True or False to match specify field value, use like below:

.. code:: python

    >>> mf = lambda v, item: v > item["out"]

    >>> pipeline = [
    ...    {
    ...        "$match": {
    ...            "in": {"$call": mf},
    ...        },
    ...    },
    ... ]

$project
~~~~~~~~
Reshapes a document stream by renaming, adding, or removing fields. Also use $project to create computed values or sub-documents. Use $project to:

- Include fields from the original document.
- Insert computed fields.
- Rename fields.
- Create and populate fields that hold sub-documents.

See this `mongo aggregation $project
<http://docs.mongodb.org/manual/reference/operator/aggregation/project/>`_ for more.

$project command support basic operators: $add, $substract, $multiply, and $divide,
in addition to this, pipestat $project command support more, like **$extract**, **$timestamp**, **$call**.

$extract operator use to extract field from other field use regular expression,
value first find groupdict()[FIELD], next find group(1), final use group(), use like below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$project": {
    ...            "app": {"$extract": ["$_event", "app:(\w*)"]},
    ...            "action": {"$extract": ["$_event", "(cached|refresh|locked)"]},
    ...            "elapse": {"$extract": ["$_event", "elapse:([\d.]*)"]},
    ...        },
    ...    },
    ... ]

$timestamp operator convert formatted string time to seconds float value, use like below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$project": {
    ...            "ts": {"$timestamp": ["$ts_str", "YYYY-mm-DD HH:MM:SS"]},
    ...        },
    ...    },
    ... ]

$call operator used for advance purpose if all above cannot satisfy you, use like below:

.. code:: python

    >>> slot_ts = lambda x: x["ts"] // 300 * 300

    >>> pipeline = [
    ...    {
    ...        "$project": {
    ...            "ts": {"$call": slot_ts},
    ...        },
    ...    },
    ... ]

 **pipestat** $project command do not support nest operator, so if you want complex operator, please use **$call**.

$group
~~~~~~
Groups documents together for the purpose of calculating aggregate values based on a collection of documents.
In practice, $group often supports tasks such as average page views for each page in a website on a daily basis.

See this `mongo aggregation $group
<http://docs.mongodb.org/manual/reference/operator/aggregation/group/>`_ for more.

$group command support basic operators: $sum, $min, $max, $first, $last, $addToSet, $push.
all this operators are identical to mongo corresponding operator, see a example as below:

.. code:: python

    >>> pipeline = [
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
    ... ]

$sort
~~~~~
the $sort pipeline command sorts all input documents and returns them to the pipeline in sorted order

See this `mongo aggregation $sort
<http://docs.mongodb.org/manual/reference/operator/aggregation/sort/>`_ for more.

$sort command is identical to mongo aggregation $sort,
but you should use tuple list instead of dict because python dict unordered! see a example as below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$sort": [
    ...            ("app", 1),
    ...            ("action", 1),
    ...        ]
    ...    },
    ... ]

$limit
~~~~~~
Restricts the number of documents that pass through the $limit in the pipeline.

See this `mongo aggregation $limit
<http://docs.mongodb.org/manual/reference/operator/aggregation/limit/>`_ for more.

$limit command is identical to mongo aggregation $limit, see a example as below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$limit": 3,
    ...    },
    ... ]

$skip
~~~~~
Skips over the specified number of documents that pass through the $skip in the pipeline before passing all of the remaining input.

See this `mongo aggregation $skip
<http://docs.mongodb.org/manual/reference/operator/aggregation/skip/>`_ for more.

$skip command is identical to mongo aggregation $skip, see a example as below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$skip": 3,
    ...    },
    ... ]

$unwind
~~~~~~~
Peels off the elements of an array individually, and returns a stream of documents. $unwind returns one document for every member of the unwound array within every source document.

See this `mongo aggregation $unwind
<http://docs.mongodb.org/manual/reference/operator/aggregation/unwind/>`_ for more.

$unwind command is identical to mongo aggregation $unwind, see a example as below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$unwind": "$tags",
    ...    },
    ... ]

Advance Example
-------------------------------------------------------------

for same reason, maybe you want use low-level **Pipeline** class. with it you can do multiply pipestat for same dataset.
see below example.

.. code:: python

    >>> from pipestat import Pipeline, LimitExceedError

    >>> dataset = [
    ...     {
    ...         "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... refresh, elapse:1.0",
    ...     },
    ...     {
    ...         "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... cached, elapse:0.01",
    ...     },
    ...     {
    ...         "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app40 timeline end... refresh, elapse:2.0",
    ...     },
    ... ]

    >>> pipeline1 = Pipeline([
    ...     {
    ...         "$match": {
    ...             "_event": {"$regexp": "Collect\s*app:.*timeline.*end.*elapse"},
    ...         },
    ...     },
    ...     {
    ...         "$project": {
    ...             "app": {"$extract": ["$_event", "app:(\w*)"]},
    ...             "elapse": {"$extract": ["$_event", "elapse:([\d.]*)"]},
    ...         },
    ...     },
    ...     {
    ...         "$group": {
    ...             "_id": {
    ...                 "app": "$app",
    ...             },
    ...             "count": {"$sum": 1},
    ...             "sum_elapse": {"$sum": "$elapse"},
    ...         }
    ...     },
    ...     {
    ...         "$project": {
    ...             "app": "$_id.app",
    ...             "avg_elapse": {"$divide": ["$sum_elapse", "$count"]},
    ...         },
    ...     },
    ...     {
    ...         "$sort": [
    ...             ("app", 1),
    ...         ]
    ...     },
    ... ])

    >>> pipeline2 = Pipeline([
    ...     {
    ...         "$match": {
    ...             "_event": {"$regexp": "Collect\s*app:.*timeline.*end.*elapse"},
    ...         },
    ...     },
    ...     {
    ...         "$project": {
    ...             "app": {"$extract": ["$_event", "app:(\w*)"]},
    ...             "action": {"$extract": ["$_event", "(cached|refresh|locked)"]},
    ...         },
    ...     },
    ...     {
    ...         "$group": {
    ...             "_id": {
    ...                 "app": "$app",
    ...             },
    ...             "actions": {"$addToSet": "$action"},
    ...         }
    ...     },
    ...     {
    ...         "$project": {
    ...             "app": "$_id.app",
    ...             "actions": "$actions",
    ...         },
    ...     },
    ...     {
    ...         "$sort": [
    ...             ("app", 1),
    ...         ]
    ...     },
    ... ])

    >>> pipes = [pipeline1, pipeline2]

    >>> for item in dataset:
    ...     for p in pipes:
    ...         try:
    ...             p.feed(item)
    ...         except LimitExceedError:
    ...             pipes.remove(p)

    >>> pipeline1.result()
    [
        {
            "count": 1.0,
            "avg_elapse": 0.01,
            "app": "app37",
            "action": "cached",
            "min_elapse": 0.01,
            "max_elapse": 0.01
        },
        {
            "count": 1.0,
            "avg_elapse": 1.0,
            "app": "app37",
            "action": "refresh",
            "min_elapse": 1.0,
            "max_elapse": 1.0
        },
        {
            "count": 1.0,
            "avg_elapse": 2.0,
            "app": "app40",
            "action": "refresh",
            "min_elapse": 2.0,
            "max_elapse": 2.0
        }
    ]

    >>> pipeline2.result()
    [
        {
            "app": "app37",
            "actions": [
                "cached",
                "refresh"
            ]
        },
        {
            "app": "app40",
            "actions": [
                "refresh"
            ]
        }
    ]

as you see when you use low-level **Pipeline** class, you should handle LimitExceedError by youself.
LimitExceedError is raise when you use **$limit**, and result is exceed limit count.
