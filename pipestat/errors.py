# -*- coding: utf-8 -*-

class PipeStatError(Exception):
    """pipestat base error"""

class OperatorError(Exception):
    """command operator error"""

class PipeCmdError(Exception):
    """pipe command error"""

class LimitCmdCompleted(PipeCmdError):
    """limit command completed"""
