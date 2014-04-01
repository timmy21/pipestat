# -*- coding: utf-8 -*-

class PipeStatError(Exception):
    """pipestat base error"""


class PipelineError(PipeStatError):
    """pipeline error"""


class OperatorError(PipeStatError):
    """command operator error"""

    def __init__(self, message, command, operator):
        super(OperatorError, self).__init__(message)
        self.command = command
        self.operator = operator


class CommandError(PipeStatError):
    """pipe command error"""

    def __init__(self, message, command):
        super(CommandError, self).__init__(message)
        self.command = command


class LimitCompleted(PipeStatError):
    """limit command completed"""
