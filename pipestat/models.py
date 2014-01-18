# -*- coding: utf-8 -*-

from pipestat.pipeline import Pipeline, LimitExceedError


def pipestat(dataset, pipeline):
    p = Pipeline(pipeline)
    try:
        for item in dataset:
            p.feed(item)
    except LimitExceedError:
        pass
    return p.result()

