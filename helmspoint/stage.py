import os
import sys
import hashlib
import ast
import json
import cloudpickle

from helmspoint.helmspoint import Helmspoint

# TODO split stage and the func from each other. 
# stage holds the structure of DAG. func is the object
class Stage:

    @staticmethod
    def get(digest):
        (directory, filename) = Helmspoint.digest_filepath(digest) 
        filepath = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        with open(filepath, 'rb') as f:
            raw = f.read()
        return cloudpickle.loads(raw)

    # TODO does name belong here? for now, we keep name here for easy reference
    def __init__(self, func):
        self.digest = None
        self.func = func

    def name(self):
        return self.func.__name__

    def hash(self):
        if self.digest == None:
            return self.write()
        else:
            return self.digest

    def write(self):
        stage_data = self.initial_data()
        self.digest = hashlib.sha256(stage_data).hexdigest()

        (directory, filename) = Helmspoint.digest_filepath(self.digest)

        # write it to object directory
        dst_path = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        with open(dst_path, 'wb') as fw:
            fw.write(stage_data)

        return self.digest

    def initial_data(self):
        return cloudpickle.dumps(self.func)
