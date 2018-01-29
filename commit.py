import sys
import os
from datetime import datetime
from dateutil.tz import tzutc
import dateutil.parser
import json
import hashlib

from hash_object import HashObject
from hash_tree import HashTree

# takes a snapshot of a data directory
class Commit:

    REPO_ROOT = os.path.join(os.getcwd(), ".hmpt")
    REPO_OBJ_PATH = os.path.join(REPO_ROOT, "objects")
    REPO_HEADS_PATH = os.path.join(REPO_ROOT, "refs", "heads")
    REPO_TAGS_PATH = os.path.join(REPO_ROOT, "refs", "tags")

    def __init__(self):
        self.hash_algo = hashlib.sha256()

    def init(self):
        os.makedirs(self.REPO_OBJ_PATH, exist_ok = True)
        os.makedirs(self.REPO_HEADS_PATH, exist_ok = True)
        os.makedirs(self.REPO_TAGS_PATH, exist_ok = True)

        # write head
        with open(os.path.join(self.REPO_ROOT, 'HEAD'), 'w') as f:
            f.write("refs/heads/master")

        # write master
        with open(os.path.join(self.REPO_HEADS_PATH, 'master'), 'w') as f:
            f.write("0" * 64)

    def create(self, dirpath, message):
        tree_hasher = HashTree()

        commit = self.init_commit('tree', message)
        self.link_parent_commit(commit)

        (tree_digest, tree_size) = tree_hasher.hash(dirpath)
        self.link_object(commit, tree_digest, tree_size)

        # TODO link the author

        # (stage_digest, stage_size) = pipeline_stage_hasher(filepath)
        # self.link_stage(commit, stage_digest, stage_size)

        # write commit to file
        digest = self.write_commit(commit)

        print("%s %s" % (digest[0:9], message))

        return digest

    def write_commit(self, commit):
        commit_json = json.dumps(commit).encode('UTF-8')
        self.hash_algo.update(commit_json)
        digest = self.hash_algo.hexdigest()

        directory = digest[0:2]
        filename = digest[2:]

        # make the directory if it doesn't already exist
        os.makedirs(os.path.join(self.REPO_OBJ_PATH, directory), exist_ok = True)

        # write it to object directory
        dst_path = os.path.join(self.REPO_OBJ_PATH, directory, filename)
        with open(dst_path, 'wb') as fw:
            fw.write(commit_json)

        # write hash of new commit to ref/heads/master
        with open(os.path.join(self.REPO_HEADS_PATH, "master"), 'w') as fw:
            fw.write(digest)

        return digest

    ############################

    def init_commit(self, commit_type, message):
        return {
            'data': {
                'type': commit_type,
                'timestamp': datetime.utcnow().astimezone(tzutc()).isoformat('T'),
                'message': message
            },
            'links': []
        }

    def link_parent_commit(self, commit):
        # find parent in HEAD, if it exists
        digest = self.getHeadCommit() 

        commit['links'].append({
            'name': 'parent',
            'hash': digest,
            'size': 0
        })

    def link_object(self, commit, digest, size):
        commit['links'].append({
            'name': 'object',
            'hash': digest,
            'size': size  
        })


    def getHeadCommit(self):
        with open(os.path.join(self.REPO_ROOT, 'HEAD')) as f:
            ref_file = f.read()

        with open(os.path.join(self.REPO_ROOT, ref_file)) as f:
            digest = f.read()

        return digest

if __name__ == "__main__":
    cmd = sys.argv[1]

    committer = Commit()

    if cmd == "init":
        committer.init()
    elif cmd == "create":
        dirpath = sys.argv[2]
        message = sys.argv[3]
        committer.create(dirpath, message)
    else:
        print("unrecognized command")
