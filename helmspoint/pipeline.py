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
from helmspoint.commit import Commit
from helmspoint.helmspoint import Helmspoint

class Pipeline:

    # FIXME Need to decide how much to keep in memory and how much to store
    # onto the disk
    def __init__(self):
        # list of all nodes
        # name / dag
        self.dags = {}

        # list of all nodes who have no parent
        # name / dag
        self.sources = {}

        # list of all nodes who have no children
        # name / dag
        self.sinks = {}

        self.arg_map = {}

    def append(self, func, **parent_mappings):
        # can write stage hash to disk, since it doesn't depend on anything else
        stage = Stage(func)

        parent_names = list(parent_mappings.values())

        parents = [self.dags[name] for name in parent_names]
        dag = Dag(stage, parents)
        self.arg_map[dag.stage_name()] = list(parent_mappings.keys())

        # do accounting for the sources and sinks
        if len(parent_names) == 0:
            self.sources[stage.name()] = dag
            self.sinks[stage.name()] = dag
        else:
            for parent_name in parent_names:
                if parent_name in self.sinks:
                    del self.sinks[parent_name]
            self.sinks[stage.name()] = dag

        self.dags[stage.name()] = dag

        return dag

    def run(self):
        visit_node = lambda dag_node: None
        sink_nodes = list(self.sinks.values())
        sorted_dags = self.reverse_topological_sort(visit_node, sink_nodes)

        # dag hash / data hash
        data_digests = {}

        print("-------- resulting topological sort ---------")
        for dag in sorted_dags:
            dag.print()
            dag.write()
            data_digests[dag.hash()] = dag.run(self.arg_map, data_digests)

        # create a list and add all the data_digests to it
        print(data_digests)

        # create a list and add all sink nodes of pipeline to it
        # print(data_digests)

        # create a commit with a list of data and the sink nodes
        #commit = Commit().dag_create(self.sinks)

    def reverse_topological_sort(self, visit_func, sinks):
        visited = {}
        pre_stack = []
        post_stack = []

        for sink in sinks:
            pre_stack.append(sink) 

            while len(pre_stack) != 0:
                dag_node = pre_stack.pop()

                if dag_node not in visited:
                    visited[dag_node] = True

                    pre_stack.append(dag_node)
                    for parent_dag in dag_node.parents:
                        if parent_dag not in visited:
                            pre_stack.append(parent_dag)
                else:
                    visit_func(dag_node)
                    post_stack.append(dag_node)

        return post_stack

    def status(self):
        print("--------- Dags -------------")
        print(self.dags)
        print("--------- Sources ----------")
        print(self.sources)
        print("--------- Sinks ----------")
        print(self.sinks)
