import sys
import os
import glob
import hashlib
import json
from helmspoint import Helmspoint

from blob import Blob

class Tree:

    def hash(self, dirpath):
        obj_hasher = Blob()
        total_size = 0

        tree = self.init()

        for file_path in glob.glob(os.path.join(dirpath, '*')):
            filename = self.path_leaf(file_path)

            if os.path.exists(file_path) and os.path.isfile(file_path):
                (obj_digest, obj_size) = obj_hasher.hash(file_path)
                obj_type = 'blob'
                print('blob', obj_digest[0:7], file_path, obj_size)

            elif os.path.exists(file_path) and os.path.isdir(file_path):
                (obj_digest, obj_size) = self.hash(file_path)
                obj_type = 'tree'

            tree = self.append(tree, filename, obj_type, obj_digest, obj_size)
            total_size += obj_size

        digest = self.write(tree)
        print('tree', digest[0:7], dirpath, total_size)

        return (digest, total_size)

    def path_leaf(self, path):
        head, tail = os.path.split(path)
        return tail or os.path.basename(head)

    def init(self):
        return { 'data': [], 'links': [] }

    def append(self, tree, filename, obj_type, obj_digest, obj_size):
        tree['data'].append(obj_type)
        tree['links'].append({
            'name': filename,
            'hash': obj_digest,
            'size': obj_size
        })
        return tree

    def write(self, tree):
        tree_json = json.dumps(tree).encode('UTF-8')
        digest = hashlib.sha256(tree_json).hexdigest()

        (directory, filename) = Helmspoint.digest_filepath(digest)

        # write it to object directory
        dst_path = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        with open(dst_path, 'wb') as fw:
            fw.write(tree_json)

        return digest
