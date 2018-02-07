import os
from datetime import datetime
from dateutil.tz import tzutc
import dateutil.parser
import json
import hashlib

from blob import Blob
from tree import Tree
from helmspoint import Helmspoint

# takes a snapshot of a data directory
# TODO change name to DataCommit
class Commit:

    def create(self, dirpath, message):
        tree_hasher = Tree()

        commit = self.init(message)
        self.link_parent(commit)

        (tree_digest, tree_size) = tree_hasher.hash(dirpath)
        self.link_object(commit, 'tree', tree_digest, tree_size)

        # TODO link the author

        # (stage_digest, stage_size) = pipeline_stage_hasher(filepath)
        # self.link_stage(commit, stage_digest, stage_size)

        # write commit to file
        digest = self.write(commit)

        print("%s %s" % (digest[0:9], message))

        return digest

    def write(self, commit):
        commit_json = json.dumps(commit).encode('UTF-8')
        digest = hashlib.sha256(commit_json).hexdigest()

        (directory, filename) = Helmspoint.digest_filepath(digest)

        # write it to object directory
        dst_path = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        with open(dst_path, 'wb') as fw:
            fw.write(commit_json)

        # write hash of new commit to ref/heads/master
        with open(os.path.join(Helmspoint.REPO_HEADS_PATH, "master"), 'w') as fw:
            fw.write(digest)

        return digest

    ############################

    def init(self, message):
        return {
            'data': {
                'type': 'commit',
                'timestamp': datetime.utcnow().astimezone(tzutc()).isoformat('T'),
                'message': message
            },
            'links': []
        }

    def link_parent(self, commit):
        # find parent in HEAD, if it exists
        digest = self.getHeadCommit() 

        commit['links'].append({
            'name': 'parent',
            'hash': digest,
            'size': 0
        })

    def link_object(self, commit, object_type, digest, size):
        commit['links'].append({
            'name': object_type,
            'hash': digest,
            'size': size  
        })

    def getHeadCommit(self):
        with open(os.path.join(Helmspoint.REPO_ROOT, 'HEAD')) as f:
            ref_file = f.read()

        with open(os.path.join(Helmspoint.REPO_ROOT, ref_file)) as f:
            digest = f.read()

        return digest

