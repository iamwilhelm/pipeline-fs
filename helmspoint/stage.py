import os
import sys
import inspect
import hashlib
import ast
import json

from helmspoint.helmspoint import Helmspoint

# TODO split stage and the func from each other. 
# stage holds the structure of DAG. func is the object
class Stage:

    @staticmethod
    def ast_dump(func):
        code = inspect.getsource(func)
        ast_nodes = ast.parse(code)
        return ast.dump(ast_nodes)

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
        stage_json = json.dumps(stage_data).encode('UTF-8')
        self.digest = hashlib.sha256(stage_json).hexdigest()

        (directory, filename) = Helmspoint.digest_filepath(self.digest)

        # write it to object directory
        dst_path = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        with open(dst_path, 'wb') as fw:
            fw.write(stage_json)

        return self.digest

    def initial_data(self):
        return {
            'version': sys.version,
            'name': self.func.__name__,
            'func': Stage.ast_dump(self.func)
        }
