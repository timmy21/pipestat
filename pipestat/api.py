# -*- coding: utf-8 -*-

from pipestat.pipeline import Pipeline
from pipestat.errors import LimitCmdCompleted


def pipestat(dataset, pipeline):
    p = Pipeline(pipeline)
    try:
        for item in dataset:
            p.feed(item)
    except LimitCmdCompleted:
        pass
    return p.result()
