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
    ...             "elapse": {
    ...                 "$toNumber": {
    ...                     "$extract": ["$_event", "elapse:([\d.]*)"],
    ...                 },
    ...             }
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
    ...             "elapse": {
    ...                 "min": "$min_elapse",
    ...                 "max": "$max_elapse",
    ...                 "avg": {"$divide": ["$sum_elapse", "$count"]},
    ...             },
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
    ...         "_event": "[2014-01-16 16:13:49,171] DEBUG Collect app:app37 timeline end... refresh, elapse:2.0",
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
            "app": "app37",
            "action": "cached",
            "elapse": {
                "min": 0.01,
                "max": 0.01,
                "avg": 0.01,
            }
        },
        {
            "count": 1.0,
            "app": "app37",
            "action": "refresh",
            "elapse": {
                "min": 1.0,
                "max": 2.0,
                "avg": 1.5,
            }
        },
        {
            "count": 1.0,
            "app": "app40",
            "action": "refresh",
            "elapse": {
                "min": 2.0,
                "max": 2.0,
                "avg": 2.0,
            }
        }
    ]

What commands pipestat support
---------------------------------------------------------------------------------

$match
~~~~~~

$match pipes the documents that match its conditions to the next operator in the pipeline.
See this `mongo aggregation $match
<http://docs.mongodb.org/manual/reference/operator/aggregation/match/>`_ for more.

$match command support basic operators: $and, $or, $gt, $gte, $lt, $lte, $ne, $eq, $in, $nin.
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

$call operator use callable which argument is document, and return True or False to indicate match or not, use like below:

.. code:: python

    >>> mf = lambda doc: doc["in"] > doc["out"]

    >>> pipeline = [
    ...    {
    ...        "$match": {
    ...            "$call": mf,
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

$project command support basic operators: $add, $substract, $multiply, $divide, $mod, $toLower, $toUpper, $concat and Date operators.
in addition to this, pipestat $project command support more, like **$toNumber**, **$extract**, **$timestamp**, **$call**.

$toNumber operator use to convert string to number.

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$project": {
    ...            "elapse": {"$toNumber": "$elapse"},
    ...        },
    ...    },
    ... ]

$extract operator use to extract field from other field use regular expression,
value fetch order is groupdict()["extract"] >  group(1) > group(), use like below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$project": {
    ...            "app": {"$extract": ["$_event", "app:(\w*)"]},
    ...            "action": {"$extract": ["$_event", "(cached|refresh|locked)"]},
    ...             "elapse": {
    ...                 "$toNumber": {
    ...                     "$extract": ["$_event", "elapse:([\d.]*)"],
    ...                 },
    ...             }
    ...        },
    ...    },
    ... ]

$timestamp operator convert formatted string time to seconds float value, use like below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$project": {
    ...            "ts": {"$timestamp": ["$ts_str", "%Y-%m-%d %H:%M:%S"]},
    ...        },
    ...    },
    ... ]

$call operator used for advance purpose if all above cannot satisfy you, use like below:

.. code:: python

    >>> slot_ts = lambda document: document["ts"] // 300 * 300

    >>> pipeline = [
    ...    {
    ...        "$project": {
    ...            "ts": {"$call": slot_ts},
    ...        },
    ...    },
    ... ]

pipestat $project command **support combine operator** like below:

.. code:: python

    >>> pipeline = [
    ...     {
    ...         "$project": {
    ...             "traffic": {"$divide": [{"$multiply": ["$traffic", 8]}, 1024]}
    ...         }
    ...     }
    ... ]

$group
~~~~~~
Groups documents together for the purpose of calculating aggregate values based on a collection of documents.
In practice, $group often supports tasks such as average page views for each page in a website on a daily basis.

See this `mongo aggregation $group
<http://docs.mongodb.org/manual/reference/operator/aggregation/group/>`_ for more.

$group command support basic operators: $sum, $min, $max, $first, $last, $addToSet, $push.
in addition to this, pipestat $group command support more, like **$concatToSet**, **$concatList**, **$call**.
see a example as below:

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

$concatToSet operator used to merge many list values or single values to one list which without same value.

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$group": {
    ...            "_id": {
    ...                "app": "$app",
    ...            },
    ...            "action": {"$concatToSet": "$action"},
    ...        }
    ...    },
    ... ]

$concatToList operator work same with $concatToSet but final list can have same value.

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$group": {
    ...            "_id": {
    ...                "app": "$app",
    ...            },
    ...            "action": {"$concatToList": "$action"},
    ...        }
    ...    },
    ... ]

$call operator used for advance purpose if all above cannot satisfy you, use like below:
$call is very like python built-in reduce function.
it's second paramter is accumulate result, initial value is **undefined**.

.. code:: python

    >>> def filter_concat(document, acc_val):
    ...    if(acc_val == undefined):
    ...        acc_val = []
    ...    if(document["action"] != "refresh"):
    ...        acc_val.append(document["action"])
    ...    return acc_val

    >>> pipeline = [
    ...    {
    ...        "$group": {
    ...            "_id": {
    ...                "app": "$app",
    ...            },
    ...            "action": {
    ...                "$call": filter_concat
    ...            },
    ...        }
    ...    },
    ... ]

$sort
~~~~~
the $sort pipeline command sorts all input documents and returns them to the pipeline in sorted order

See this `mongo aggregation $sort
<http://docs.mongodb.org/manual/reference/operator/aggregation/sort/>`_ for more.

$sort command is identical to mongo aggregation $sort,
not only use dict, you also can use a list of tuple or collections.OrderedDict, for multi-key sort order reason! see a example as below:

.. code:: python

    >>> pipeline = [
    ...    {
    ...        "$sort": {"app": 1}
    ...    },
    ... ]

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
