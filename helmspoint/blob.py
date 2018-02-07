import sys
import os
import hashlib
import uuid
import json

from helmspoint.helmspoint import Helmspoint

class Blob:

    BLOCKSIZE = 65536

    def __init__(self):
        self.hash_algo = hashlib.sha256()

    def hash(self, filepath):
        tmp_filename = str(uuid.uuid4())
        total_size = 0

        with open(os.path.join("/tmp", tmp_filename), 'wb') as fw:
            with open(filepath, 'rb') as fr:
                # read file block by block
                buf = fr.read(self.BLOCKSIZE)
                buf_len = len(buf)
                total_size += buf_len

                while buf_len > 0:
                    self.hash_algo.update(buf)
                    fw.write(buf)

                    buf = fr.read(self.BLOCKSIZE)
                    buf_len = len(buf)
                    total_size += buf_len

        # figure out the digest of the file
        digest = self.hash_algo.hexdigest()
        (directory, filename) = Helmspoint.digest_filepath(digest)

        # move the file
        obj_src = os.path.join("/tmp", tmp_filename)
        obj_dst = os.path.join(Helmspoint.REPO_OBJ_PATH, directory, filename)
        os.rename(obj_src, obj_dst)

        return digest, total_size

