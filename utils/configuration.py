# Copyright (c) Microsoft Corporation.

import json

class Generic(object):
    pass

class Config:
    def __init__(self, config_json):

        self.raw_config = {}
        with open(config_json,"r") as input_config:
            file_data = input_config.readlines()
            self.raw_config = json.loads("\n".join(file_data))

        for key in self.raw_config.keys():
            self._generate_config(self, key, self.raw_config[key] )

    def _generate_config(self, parent, name, value):

        if not isinstance(value, dict):
            setattr(parent, name, value)
        else:
            gen_child = Generic()
            for key in value.keys():
                self._generate_config(gen_child, key, value[key])
            setattr(parent, name, gen_child)                            
