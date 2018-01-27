import ray
import numpy as np

MAX_SUBLIST_SIZE = 10


class LocalEdges(object):
    """
    This object contains a list of edges (both local and in Ray's object store)
    corresponding to a node.

    @field edges: list of OIDs (each OID represents a list of edges in Ray).
    @field buf: list of edges not yet flushed to Ray.
    """
    def __init__(self, *edges):
        """Constructor for LocalEdges class

        @param edges: One or more edges to initialize this object.
        """
        self.edges = \
            [obj for obj in edges if type(obj) is ray.local_scheduler.ObjectID]

        self.buf = \
            np.array([obj for obj in edges
                      if type(obj) is not ray.local_scheduler.ObjectID])
        if len(self.buf) > MAX_SUBLIST_SIZE:
            self.edges.append(ray.put(self.buf))
            self.buf = np.array([])

    def append(self, *values):
        """Append new values to the edge list.

        @param values: values are edge objects.

        @return: A list of edges including the new appended values.
        """
        temp_edges = self.edges[:]
        temp_edges.extend([val for val in values
                           if type(val) is ray.local_scheduler.ObjectID])
        temp_buf = \
            np.append(self.buf,
                      [val for val in values
                       if type(val) is not ray.local_scheduler.ObjectID])

        if len(temp_buf) > MAX_SUBLIST_SIZE:
            temp_edges.append(ray.put(self.buf))
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


@ray.remote
def _filter_remote(filterfn, chunk):
    """Apply a filter function to an object.

    @param filterfn: The filter function to be applied to the list of
                     edges.
    @param chunk: The object to which the filter will be applied.

    @return: An array of filtered objects.
    """
    return np.array(filter(filterfn, chunk))
