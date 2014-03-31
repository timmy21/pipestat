# -*- coding: utf-8 -*-

from pipestat.commands import CommandFactory
from pipestat.errors import CommandError
from pipestat.models import Document


class Pipeline(object):

    def __init__(self, pipeline):
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
            raise CommandError('the pipeline requires an array of commands')

    def feed(self, item):
        self.cmd.feed(Document(item))

    def result(self):
        return self.cmd.result()
