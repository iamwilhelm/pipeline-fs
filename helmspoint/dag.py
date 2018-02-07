import os
import json
import hashlib

from helmspoint.helmspoint import Helmspoint

class Dag:

    def __init__(self, stage_name, stage_digest):
        self.data = {
            'data': {
                'type': 'dag'
            },
            'links': [{
                'name': stage_name, # name or type here?
                'hash': stage_digest
            }]
        }
        self.digest = None

    def set_upstream(self, func_digest):
        self.data['links'].append({
            'name': 'parent',
            'hash': func_digest
        })

    # FIXME should calculating the hash be the same as writing to disk?
    def hash(self):
        if self.digest == None:
            self.digest = self.write()
        return self.digest

    def write(self):
        print(self.data)
        dag_json = json.dumps(self.data).encode('UTF-8')
        digest = hashlib.sha256(dag_json).hexdigest()

        (directory, filename) = Helmspoint.digest_filepath(digest)

        dst_path = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        with open(dst_path, 'wb') as fw:
            fw.write(dag_json)

        return digest



