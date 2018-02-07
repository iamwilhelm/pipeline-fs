import os
import sys
import inspect
import hashlib
import ast
import json
from datetime import datetime
from dateutil.tz import tzutc
import dateutil.parser

from helmspoint.stage import Stage
from helmspoint.dag import Dag

class Pipeline:

    @staticmethod
    def genesis_hash():
        return "0" * 64

    def __init__(self):
        # list of all nodes
        # name / dag
        self.dags = {}

        # list of all nodes who have no parent
        # name / name
        self.sources = {}

        # list of all nodes who have no children
        # name / name
        self.sinks = {}

    def append(self, func, parent_names = []):
        stage = Stage(func)
        dag = Dag(stage.name(), stage.hash())

        if len(parent_names) == 0:
            dag.set_upstream(Pipeline.genesis_hash()) 
            self.sources[stage.name()] = stage.name()
            self.sinks[stage.name()] = stage.name()
        else:
            for parent_name in parent_names:
                parent_hash = self.dags[parent_name].hash()
                dag.set_upstream(parent_hash)

                # remove parent from list of sinks
                if parent_name in self.sinks:
                    del self.sinks[parent_name]

            # add current node to list of sinks
            self.sinks[stage.name()] = stage.name()

        digest = dag.hash()
        self.dags[stage.name()] = dag

        print("%s %s" % (digest[0:7], stage.name()))
        return

    def status(self):
        print("--------- Sources ----------")
        print(self.sources)
        print("--------- Sinks ----------")
        print(self.sinks)

    def commit(self):
        print("commit this shit")

    def run(self):
        print("run and commit?")


