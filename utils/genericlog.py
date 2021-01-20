# Copyright (c) Microsoft Corporation.

import os
from datetime import datetime


class Logger:

    @staticmethod
    def add_log(message, controller_log='storage.log'):
        """
        Very simple logger that just dumps out the message with a time stamp
        with append only. Careful this could create an enormous log.
        """
        output = "\n{}\t{}".format(str(datetime.now()), message)
        with open(controller_log, 'a', encoding="utf-8") as log_file:
            try:
                log_file.writelines(output)
            except Exception as ex:
                output = "\n{}\t{}".format(str(datetime.now()), "LOG ERROR: {}".format(str(ex)))
                log_file.write(output)

    @staticmethod
    def clear_log(max_size=2097152, controller_log='storage.log'):
        """
        Checks the size of the log and renames it if it is already
        a certain size.
        """
        if os.path.exists(controller_log):
            size = os.path.getsize(controller_log)
            print("Log is {} bytes".format(size))
            if size >= max_size:
                os.remove(controller_log)
                Logger.add_log("Log file cleared on size... {}".format(size))

