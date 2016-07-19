#!/usr/bin/env python
# -*- cod.ing: utf8 -*-

"""Classes, funções e macros úteis."""


HEADER = '\033[95m'
OK = '\033[92m'
INFO = '\033[94m'
WARN = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'


class Messages:
    """Messages definition for display on the terminal."""

    def __init__(self):
        """Initialization of variables."""
        self.OK = OK + "[ OK ]" + ENDC
        self.INFO = INFO + "[INFO]" + ENDC
        self.WARN = WARN + "[WARN]" + ENDC
        self.FAIL = FAIL + "[FAIL]" + ENDC

    def ok(self, msg, tab=""):
        """Success message."""
        print(tab + self.OK + " " + msg)

    def info(self, msg, tab=""):
        """Information message."""
        print(tab + self.INFO + " " + msg)

    def warn(self, msg, tab=""):
        """Warning message."""
        print(tab + self.WARN + " " + msg)

    def fail(self, msg, tab=""):
        """Failure message."""
        print(tab + self.FAIL + " " + msg)
