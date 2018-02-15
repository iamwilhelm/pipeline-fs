import os
import json
import hashlib

from helmspoint.helmspoint import Helmspoint
from helmspoint.stage import Stage
from helmspoint.blob import Blob

# TODO dag and stage has the same write()
# TODO get() should be the same also? 
class Dag:

    @staticmethod
    def get(digest):
        (directory, filename) = Helmspoint.digest_filepath(digest) 
        filepath = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        with open(filepath, 'rb') as f:
            raw = f.read()
        return json.loads(raw)

    def __init__(self, stage, parents = []):
        self.stage = stage
        self.parents = parents
        self.digest = None

        # name / blob hash
        # This need to be written to disk or something. maybe by pipeline as a commit?
        self.data_map = {}

    def set_upstream(self, parents):
        self.parents.extend(parents)

    # NOTE should do the deserialization, because in a decentralized system,
    # you'll need to read from disk or database anyway, when you have lots
    # of different workers
    def run(self, arg_map, data_digests):
        # get the dag
        dag_json = Dag.get(self.digest)

        # get the stage
        func_link = next(link for link in dag_json['links'] if link['name'] == 'func')

        # deserialize the func
        stage_func = Stage.get(func_link['hash'])

        # use every parent dag hash to look up data.
        parent_links = [link for link in dag_json['links'] if link['name'] == 'parent']
        parent_dag_digests = map(lambda link: link['hash'], parent_links)

        print("arg_map %s" % arg_map)
        print("data_digests %s" % data_digests)
        print("parent_dag_digests %s" % parent_dag_digests)

        # build up data arguments to go into this stage
        arg_names = arg_map[dag_json['data']['name']]
        input_data = []
        for parent_data_digest in parent_dag_digests:
            data_digest = data_digests[parent_data_digest]
            (directory, filepath) = Helmspoint.digest_filepath(data_digest)
            datapath = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filepath)
            with open(datapath, 'r') as f:
                raw_data = f.read()
                json_data = json.loads(raw_data)
                input_data.append(json_data)

        # run it
        print("running stage: %s" % dag_json['data']['name'])
        parents_mapping = dict(zip(arg_names, input_data))
        output_data = stage_func(**parents_mapping)

        # hash data and write the data to disk
        pipe_result_path= os.path.join("datasource", "pipeline")
        os.makedirs(pipe_result_path, exist_ok = True)
        datapath = os.path.join(pipe_result_path, dag_json['data']['name'])
        with open(datapath, 'w') as f:
            json_data = json.dumps(output_data)
            f.write(json_data)
        (data_digest, data_size) = Blob().hash(datapath)

        print("----------")

        return data_digest

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
        dag_data = self._build_initial_data()
        for parent in self.parents:
            dag_data = self._build_parent_link(dag_data, parent) 
        return dag_data

    def _build_initial_data(self):
        self.stage.write()
        return {
            'data': {
                'type': 'dag',
                'name': self.stage.name()
            },
            'links': [{
                'name': 'func',
                'hash': self.stage.hash()
            }]
        }

    def _build_parent_link(self, dag_data, parent_dag):
        parent_dag.write()
        dag_data['links'].append({
            'name': 'parent',
            'hash': parent_dag.hash()
        })
        return dag_data

