import sys
import os
import hashlib
import uuid

class HashObject:

    BLOCKSIZE = 65536
    REPO_ROOT = os.path.join(os.getcwd(), ".hmpt")
    REPO_OBJ_PATH = os.path.join(REPO_ROOT, "objects")

    def __init__(self):
        self.hash_algo = hashlib.sha256()

    def hash(self, filepath):
        tmp_filename = str(uuid.uuid4())
        total_size = 0

        with open(filepath, 'rb') as fr:
            with open(os.path.join("/tmp", tmp_filename), 'wb') as fw:
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
        directory = digest[0:2]
        filename = digest[2:]

        # make the directory if it doesn't already exist
        os.makedirs(os.path.join(self.REPO_OBJ_PATH, directory), exist_ok = True)

        # rename the file
        obj_src = os.path.join("/tmp", tmp_filename)
        obj_dst = os.path.join(self.REPO_OBJ_PATH, directory, digest)
        if not os.path.exists(obj_dst):
            os.rename(obj_src, obj_dst)
        # else:
        #     print("Object already exists in repo")

        return digest, total_size

if __name__ == "__main__":
    if len(sys.argv) == 1:
        raise Exception("Need filename")
    filepath = sys.argv[1]
    HashObject().hash(filepath)