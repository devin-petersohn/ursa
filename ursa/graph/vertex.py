import ray
from .local_edges import LocalEdges
from .utils import _apply_filter
from .utils import _apply_append


class _Vertex(object):
    def __init__(self,
                 vertex_data=None,
                 local_edges=[],
                 foreign_edges={},
                 transaction_id=-1):
        """Contains all data for a vertex of the Graph Database.

        @param vertex_data: The metadata for this vertex.
        @param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs.
        @param foreign_edges: A dictionary of edges between graphs.
        @param transaction_id: The transaction_id that generated this vertex.
        """
        # The only thing we keep as its actual value is the None to filter
        if vertex_data is not None:
            # We need to put it in the Ray store if it's not there already.
            if type(vertex_data) is not ray.local_scheduler.ObjectID:
                self.vertex_data = ray.put(vertex_data)
            else:
                self.vertex_data = vertex_data
        else:
            self.vertex_data = vertex_data

        if type(local_edges) is LocalEdges:
            self.local_edges = local_edges
        elif len(local_edges) == 0:
            self.local_edges = LocalEdges()
        else:
            self.local_edges = LocalEdges(local_edges)

        for key in foreign_edges:
            if type(foreign_edges[key]) is not ray.local_scheduler.ObjectID:
                try:
                    foreign_edges[key] = ray.put(set([foreign_edges[key]]))
                except TypeError:
                    foreign_edges[key] = ray.put(set(foreign_edges[key]))

        self.foreign_edges = foreign_edges
        self._transaction_id = transaction_id

    def filter_local_edges(self, filterfn, transaction_id):
        """Filter the local keys based on the provided filter function.

        @param filterfn: The function to use to filter the keys.
        @param transaction_id: The system provided transaction id number.

        @return:  A new _Vertex object containing the filtered keys.
        """
        assert transaction_id >= self._transaction_id, \
            "Transactions arrived out of order."

        return self.copy(local_edges=self.local_edges.filter(filterfn),
                         transaction_id=transaction_id)

    def filter_foreign_edges(self, filterfn, transaction_id, *graph_ids):
        """Filter the foreign keys keys based on the provided filter function.

        @param filterfn: The function to use to filter the keys.
        @param transaction_id: The system provided transaction id number.
        @param graph_ids: One or more graph ids to apply the filter to.

        @return: A new _Vertex object containing the filtered keys.
        """
        assert transaction_id >= self._transaction_id, \
            "Transactions arrived out of order."

        if transaction_id > self._transaction_id:
            # we are copying for the new transaction id so that we do not
            # overwrite our previous history.
            new_edges = self.foreign_edges.copy()
        else:
            new_edges = self.foreign_edges

        for graph_id in graph_ids:
            new_edges[graph_id] = _apply_filter.remote(filterfn,
                                                       new_edges[graph_id])

        return self.copy(foreign_edges=new_edges,
                         transaction_id=transaction_id)

    def add_local_edges(self, transaction_id, *values):
        """Append to the local keys based on the provided.

        @param transaction_id: The system provided transaction id number.
        @param values: One or more values to append to the local keys.

        @return: A new _Vertex object containing the appended keys.
        """
        assert transaction_id >= self._transaction_id,\
            "Transactions arrived out of order."

        return self.copy(local_edges=self.local_edges.append(*values),
                         transaction_id=transaction_id)

    def add_foreign_edges(self, transaction_id, values):
        """Append to the local edges based on the provided.

        @param transaction_id: The system provided transaction id number.
        @param values: A dict of {graph_id: set(keys)}.

        @return: A new _Vertex object containing the appended keys.
        """
        assert transaction_id >= self._transaction_id, \
            "Transactions arrived out of order."
        assert type(values) is dict, \
            "Foreign keys must be dicts: {destination_graph: key}"

        if transaction_id > self._transaction_id:
            # we are copying for the new transaction id so that we do not
            # overwrite our previous history.
            new_edges = self.foreign_edges.copy()
        else:
            new_edges = self.foreign_edges

        for graph_id in values:
            if graph_id not in new_edges:
                new_edges[graph_id] = values[graph_id]
            else:
                new_edges[graph_id] = _apply_append.remote(new_edges[graph_id],
                                                           values[graph_id])

        return self.copy(foreign_edges=new_edges,
                         transaction_id=transaction_id)

    def copy(self,
             vertex_data=None,
             local_edges=None,
             foreign_edges=None,
             transaction_id=None):
        """Create a copy of this object and replace the provided fields.

        @param vertex_data: The metadata for this vertex.
        @param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs.
        @param foreign_edges: A dictionary of edges between graphs.
        @param transaction_id: The system provided transaction id number.

        @return: A new _Vertex object containing the copy.
        """
        if vertex_data is None:
            vertex_data = self.vertex_data
        if local_edges is None:
            local_edges = self.local_edges
        if foreign_edges is None:
            foreign_edges = self.foreign_edges
        if transaction_id is None:
            transaction_id = self._transaction_id

        return _Vertex(vertex_data, local_edges, foreign_edges, transaction_id)

    def vertex_exists(self):
        """Determine if a vertex exists.

        @return: True if vertex_data is not None, false otherwise.
        """
        return self.vertex_data is not None

    def clean_local_edges(self):
        """Sorts and removes duplicates from all the local edges for a given vertex.

        @return: A new _Vertex object with globally sorted local edges.
        """
        new_local_edges = self.local_edges.merge_common_partitions()
        return self.copy(local_edges=new_local_edges)


class _DeletedVertex(_Vertex):
    def __init__(self, transaction_id):
        """Contains all data for a deleted vertex.

        @param transaction_id: The transaction ID for deleting the vertex.
        """
        super(_DeletedVertex, self).__init__(transaction_id=transaction_id)
