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

    def __init__(self, func):
        self.data = {
            'version': sys.version,
            'name': func.__name__,
            'func': Stage.ast_dump(func)
        }

    def name(self):
        return self.data['name']

    def hash(self):
        stage_json = json.dumps(self.data).encode('UTF-8')
        digest = hashlib.sha256(stage_json).hexdigest()

        (directory, filename) = Helmspoint.digest_filepath(digest)

        # write it to object directory
        dst_path = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        with open(dst_path, 'wb') as fw:
            fw.write(stage_json)

        return digest

