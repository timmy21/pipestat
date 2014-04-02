# -*- coding: utf-8 -*-

import copy
from pipestat.commands import CommandFactory
from pipestat.errors import PipelineError
from pipestat.models import Document


class Pipeline(object):

    def __init__(self, pipeline):
        pipeline = copy.deepcopy(pipeline)
        self.cmd = None
        prev_cmd = None
        for p in pipeline:
            cmd = CommandFactory.new(p)
            if prev_cmd:
                prev_cmd.next = cmd
            else:
                self.cmd = cmd
            prev_cmd = cmd

        if not self.cmd:
            raise PipelineError('pipeline specification must be an array of at least one command')

    def feed(self, item):
        self.cmd.feed(Document(item))

    def result(self):
        return self.cmd.result()
