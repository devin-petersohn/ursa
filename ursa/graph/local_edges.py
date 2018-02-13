import ray
import numpy as np
from .utils import _filter_remote
import collections

MAX_SUBLIST_SIZE = 10
SAMPLING_COEFFICIENT = 0.1


class LocalEdges(object):
    """
    This object contains a list of edges (both local and in Ray's object store)
    corresponding to a node.

    @field edges: List of OIDs (each OID represents a list of edges in Ray).
    @field buf: List of edges not yet flushed to Ray.
    """
    def __init__(self, *edges):
        """Constructor for LocalEdges class

        @param edges: One or more edges to initialize this object.
        """
        self.edges = []
        self.buf = np.array([])
        for obj in edges:
            if type(obj) is ray.local_scheduler.ObjectID:
                self.edges.append(obj)
            elif type(obj) is list:
                for item in obj:
                    if type(item) is ray.local_scheduler.ObjectID:
                        self.edges.append(item)
                    else:
                        self.buf = np.append(self.buf, item)
            else:
                self.buf = np.append(self.buf, obj)

        if len(self.buf) >= MAX_SUBLIST_SIZE:
            self.edges.append(ray.put(self.buf))
            self.buf = np.array([])

    def append(self, *values):
        """Append new values to the edge list.

        @param values: Values are edge objects.

        @return: A list of edges including the new appended values.
        """
        temp_edges = self.edges[:]
        # TODO: might need to change the below assigments to how it is done in
        # the __init__ function
        temp_edges.extend([val for val in values
                           if type(val) is ray.local_scheduler.ObjectID])
        temp_buf = \
            np.append(self.buf,
                      [val for val in values
                       if type(val) is not ray.local_scheduler.ObjectID])

        if len(temp_buf) >= MAX_SUBLIST_SIZE:
            temp_buf = self.prune_buffer(temp_buf)
            temp_buf.sort()
            temp_edges.append(ray.put(temp_buf))
            temp_buf = np.array([])

        temp_edges.extend(temp_buf)
        return temp_edges

    def filter(self, filterfn):
        """Apply a filter function to the list of edges.

        @param filterfn: The filter function to be applied to the list of
                         edges.
        @return: The filtered list of edges.
        """
        new_edges = [_filter_remote.remote(filterfn, chunk)
                     for chunk in self.edges]
        new_buf = np.array(filter(filterfn, self.buf))
        new_edges.extend(new_buf)

        return new_edges

    def prune_buffer(self, input_buffer):
        temp_set = set()
        temp_buf = np.array([])

        for edge in input_buffer:
            if edge in temp_set:
                continue
            temp_set.add(edge)
            temp_buf = np.append(temp_buf, edge)

        temp_set = None
        return temp_buf

    def get_global_sampling(self):
        num_samples = int(SAMPLING_COEFFICIENT * MAX_SUBLIST_SIZE)
        sample_list = [np.random.choice(ray.get(list_oid), num_samples)
                       for list_oid in self.edges]
        sample_list = self.flatten(sample_list)
        sample_list.sort()
        return sample_list

    def get_partitions(self):
        indices = [int(i*(SAMPLING_COEFFICIENT * MAX_SUBLIST_SIZE))
                   for i in range(len(self.edges))]
        sample_list = self.get_global_sampling()
        return [sample_list[i] for i in indices]

    def partition_sublists(self, list_oid, partition_bounds):
        partitioned_sublist = [[] for i in range(len(partition_bounds) + 1)]
        for edge in ray.get(list_oid):
            if edge <= partition_bounds[0]:
                partitioned_sublist[0].append(edge)
            if edge > partition_bounds[-1]:
                partitioned_sublist[-1].append(edge)
            for i in range(1, len(partition_bounds)):
                if edge > partition_bounds[i - 1] and \
                       edge <= partition_bounds[i]:
                    partitioned_sublist[i].append(edge)
        return partitioned_sublist

    def merge_common_partitions(self):
        # TODO: How to send this function to a remote object - is it just pass
        # oid as argument?
        if len(self.edges) == 0:
            self.buf.sort()
            return self.buf

        new_local_edges = []

        partition_bounds = self.get_partitions()
        merged_oid_groupings = [self.partition_sublists(list_oid,
                                partition_bounds) for list_oid in self.edges]

        for i in range(len(merged_oid_groupings[0])):
            new_partition = []
            for j in range(len(merged_oid_groupings)):
                new_partition.extend(merged_oid_groupings[j][i])
            new_partition.sort()
            new_partition_oid = ray.put(new_partition)
            new_local_edges.append(new_partition_oid)
        new_local_edges.extend(self.buf)
        return new_local_edges

    def flatten(self, x):
        result = []
        for el in x:
            if isinstance(x, collections.Iterable) and not isinstance(el, str):
                result.extend(self.flatten(el))
            else:
                result.append(el)
        return result
