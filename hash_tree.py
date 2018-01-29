import sys
import os
import glob
import hashlib
import json

from hash_object import HashObject

class HashTree:

    REPO_ROOT = os.path.join(os.getcwd(), ".hmpt")
    REPO_OBJ_PATH = os.path.join(REPO_ROOT, "objects")

    def __init__(self):
        self.hash_algo = hashlib.sha256()

    def hash(self, dirpath):
        obj_hasher = HashObject()
        total_size = 0

        tree = self.init_tree()

        for file_path in glob.glob(os.path.join(dirpath, '*')):
            filename = self.path_leaf(file_path)

            if os.path.exists(file_path) and os.path.isfile(file_path):
                (obj_digest, obj_size) = obj_hasher.hash(file_path)
                obj_type = 'blob'
                print(obj_type, file_path, obj_digest, obj_size)

            elif os.path.exists(file_path) and os.path.isdir(file_path):
                (obj_digest, obj_size) = self.hash(file_path)
                obj_type = 'tree'

            tree = self.update_tree(tree, filename, obj_type, obj_digest, obj_size)
            total_size += obj_size

        digest = self.write_tree(tree)
        print('tree', dirpath, digest, total_size)

        return (digest, total_size)

    def path_leaf(self, path):
        head, tail = os.path.split(path)
        return tail or os.path.basename(head)

    def init_tree(self):
        return { 'data': [], 'links': [] }

    def update_tree(self, tree, filename, obj_type, obj_digest, obj_size):
        tree['data'].append(obj_type)
        tree['links'].append({
            'name': filename,
            'hash': obj_digest,
            'size': obj_size
        })
        return tree

    def write_tree(self, tree):
        tree_json = json.dumps(tree).encode('UTF-8')

        self.hash_algo.update(tree_json)
        digest = self.hash_algo.hexdigest()

        directory = digest[0:2]
        filename = digest[2:]

        # make the directory if it doesn't already exist
        os.makedirs(os.path.join(self.REPO_OBJ_PATH, directory), exist_ok = True)

        # write it to object directory
        dst_path = os.path.join(self.REPO_OBJ_PATH, directory, filename)
        with open(dst_path, 'wb') as fw:
            fw.write(tree_json)

        return digest


if __name__ == "__main__":
    if len(sys.argv) == 1:
        raise Exception("Need dirname")
    dirpath = sys.argv[1]
    HashTree().hash(dirpath)