import os
import json
import hashlib

from helmspoint.helmspoint import Helmspoint

# TODO dag and stage has the same write()
# TODO get() should be the same also? 
class Dag:

    @staticmethod
    def get(digest):
        (directory, filename) = Helmspoint.digest_filepath(digest) 
        filepath = os.path.join(directory, filename)
        with open(filepath, 'rb') as f:
            raw = f.read()
        return json.loads(raw)

    @staticmethod
    def is_source(dag_data):
        parent_links = []
        for x in dag_data['links']:
            if x['name'] == 'parent' and x['hash'] == Helmspoint.genesis_digest():
                parent_links.append(x)
        return len(parent_links) > 0

    def __init__(self, stage, parents = []):
        self.stage = stage
        self.parents = parents
        self.digest = None

    def set_upstream(self, parents):
        self.parents.extend(parents)

    # NOTE should do the deserialization, because in a decentralized system,
    # you'll need to read from disk or database anyway, when you have lots
    # of different workers
    def run(self):
        # get the func
        # deserialize the func
        # run it
        # hash the resulting data and write to disk (update hash?)
        return None

    def print(self):
        print("dag: %s" % self.stage_name())
        for parent in self.parents:
            print("  parent: %s" % parent.stage_name())

    def stage_name(self):
        return self.stage.name()

    def hash(self):
        if self.digest == None:
            return self.write()
        else:
            return self.digest

    def write(self):
        dag_data = self.build()
        dag_json = json.dumps(dag_data).encode('UTF-8')
        self.digest = hashlib.sha256(dag_json).hexdigest()

        (directory, filename) = Helmspoint.digest_filepath(self.digest)

        dst_path = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        with open(dst_path, 'wb') as fw:
            fw.write(dag_json)

        return self.digest

    ##### private

    def build(self):
        dag_data = self.initial_data()
        for parent in self.parents:
            dag_data = self.build_parent_link(dag_data, parent) 
        return dag_data

    def initial_data(self):
        self.stage.write()
        return {
            'data': {
                'type': 'dag'
            },
            'links': [{
                'name': 'func',
                'hash': self.stage.hash()
            }]
        }

    def build_parent_link(self, dag_data, parent_dag):
        parent_dag.write()
        dag_data['links'].append({
            'name': 'parent',
            'hash': parent_dag.hash()
        })
        return dag_data

