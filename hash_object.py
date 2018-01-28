import sys
import os
import hashlib
import uuid

filepath = sys.argv[1]

BLOCKSIZE = 65536
REPO_ROOT = os.path.join(os.getcwd(), ".hmpt")
REPO_OBJ_PATH = os.path.join(REPO_ROOT, "objects")

os.makedirs(REPO_OBJ_PATH, exist_ok = True)
tmp_filename = str(uuid.uuid4())

hash_algo = hashlib.sha256()
with open(filepath, 'rb') as fr:
    with open(os.path.join("/tmp", tmp_filename), 'wb') as fw:
        # read file block by block
        buf = fr.read(BLOCKSIZE)
        while len(buf) > 0:
            hash_algo.update(buf)
            fw.write(buf)
            buf = fr.read(BLOCKSIZE)

# figure out the digest of the file
digest = hash_algo.hexdigest() 
directory = digest[0:2]
filename = digest[2:]

# make the directory if it doesn't already exist
os.makedirs(os.path.join(REPO_OBJ_PATH, directory))

# rename the file
obj_src = os.path.join("/tmp", tmp_filename)
obj_dst = os.path.join(REPO_OBJ_PATH, directory, digest)
os.rename(obj_src, obj_dst)



