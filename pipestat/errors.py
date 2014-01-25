# -*- coding: utf-8 -*-

class PipeStatError(Exception):
    """pipestat base error"""

class OperatorError(PipeStatError):
    """command operator error"""

class CommandError(PipeStatError):
    """pipe command error"""

class LimitCompleted(PipeStatError):
    """limit command completed"""
