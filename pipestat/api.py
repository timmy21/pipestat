# -*- coding: utf-8 -*-

from pipestat.pipeline import Pipeline
from pipestat.errors import LimitCompleted


def pipestat(dataset, pipeline):
    p = Pipeline(pipeline)
    try:
        for item in dataset:
            p.feed(item)
    except LimitCompleted:
        pass
    return p.result()
