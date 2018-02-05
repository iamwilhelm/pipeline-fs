import sys
import os

class Helmspoint:
    REPO_ROOT = os.path.join(os.getcwd(), ".hmpt")
    REPO_OBJ_PATH = os.path.join(REPO_ROOT, "objects")
    REPO_HEADS_PATH = os.path.join(REPO_ROOT, "refs", "heads")
    REPO_TAGS_PATH = os.path.join(REPO_ROOT, "refs", "tags")

    GENESIS_HASH = "0" * 64

    @staticmethod
    def init():
        os.makedirs(Helmspoint.REPO_OBJ_PATH, exist_ok = True)
        os.makedirs(Helmspoint.REPO_HEADS_PATH, exist_ok = True)
        os.makedirs(Helmspoint.REPO_TAGS_PATH, exist_ok = True)

        # write head
        head_file = os.path.join(Helmspoint.REPO_ROOT, 'HEAD')
        master_file = os.path.join(Helmspoint.REPO_HEADS_PATH, 'master')
        if os.path.exists(head_file) or os.path.exists(master_file):
            print("Helmspoint data already exists")
            return

        with open(head_file, 'w') as f:
            f.write("refs/heads/master")

        with open(master_file, 'w') as f:
            f.write(Helmspoint.GENESIS_HASH)

    @staticmethod
    def digest_filepath(digest):
        directory = digest[0:2]
        filename = digest[2:]

        # make the directory if it doesn't already exist
        os.makedirs(os.path.join(Helmspoint.REPO_OBJ_PATH, directory), exist_ok = True)

        return directory, filename

