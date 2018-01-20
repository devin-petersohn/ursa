import ray


class LocalEdges(object):
    """
    This object contains a list of edges (both local and in
    Ray's object store) corresponding to a node.

    Fields:
    edges -- list of OID's (each OID respresents a list of edges in Ray)
    buf -- list of edges not yet flushed to Ray
    """

    MAX_SUBLIST_SIZE = 10

    def __init__(self, edges=[]):
        if type(edges) is not ray.local_scheduler.ObjectID:
            if isinstance(edges, list) and all(isinstance(
                    edge, ray.local_scheduler.ObjectID) for edge in edges):
                self.edges = edges
            else:
                try:
                    self.edges = [ray.put(set([edges]))]
                except TypeError:
                    self.edges = [ray.put(set(edges))]
        else:
            self.edges = [edges]

        self.buf = []

    def add_local_edges(self, transaction_id, *values):
        """
        values are edge objects
        """
        for value in values:
            self.buf.append(value)
            if len(self.buf) >= self.MAX_SUBLIST_SIZE:
                new_oid = ray.put(self.buf)
                self.edges.append(new_oid)
                self.buf = []

        return self.copy(self.edges)

    def get_edges_oids(self):
        return self.edges

    def get_buffered_edges(self):
        return self.buf

    def copy(self, edges=[]):
        return LocalEdges(edges)
