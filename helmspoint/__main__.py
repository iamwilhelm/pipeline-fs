import sys
from helmspoint import Helmspoint
from blob import Blob
from tree import Tree
from commit import Commit

cmd = sys.argv[1]

#### Plumbing

if cmd == "hash_blob":
    if len(sys.argv) == 1:
        raise Exception("Need filename")
    filepath = sys.argv[2] 
    Blob().hash(filepath)

elif cmd == "hash_tree":
    if len(sys.argv) == 1:
        raise Exception("Need dirname")
    dirpath = sys.argv[2]
    Tree().hash(dirpath)

#### Porcelain

elif cmd == "init":
    Helmspoint.init()

elif cmd == "commit":
    dirpath = sys.argv[2]
    message = sys.argv[3]
    Commit().create(dirpath, message)

else:
    print("unrecognized command")
