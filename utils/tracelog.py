# Copyright (c) Microsoft Corporation.

import os
from datetime import datetime
from utils.genericlog import Logger

class FunctionTrace:
    def __init__(self, function):
        self.function = function

    def __call__(self, *args, **kwargs):

        # Funciton return value
        return_value = None

        # Start time
        function_start = datetime.now()

        # Base message
        spacer = '\t' * 8
        out_message_base = "Module: {} - Function: {} ".format(
            self.function.__module__,
            self.function.__name__)

        out_message_base += "\n{}ARGUMENTS: {}".format(spacer, args)

        try:
            # Execute funciton, if exception log it
            return_value = self.function(*args, **kwargs)
        except Exception as ex:
            out_message_base += "\n{}EXCEPTION: {}".format(spacer, str(ex))

        # Add function return
        out_message_base += "\n{}RETURNS: {}".format(spacer, return_value)

        # Add clock to function
        span = datetime.now() - function_start
        out_message_base += "\n{}EXECUTION: {}".format(spacer, str(span))

        # Finally log it and return the function return value
        Logger.add_log(out_message_base)

        return return_value
