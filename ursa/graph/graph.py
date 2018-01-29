import ray
from .local_edges import LocalEdges
from .utils import _apply_filter
from .utils import _apply_append


@ray.remote(num_cpus=2)
class Graph(object):
    """This object contains reference and connection information for a graph.

    @field vertices: The dictionary of _Vertex objects.
    """
    def __init__(self, transaction_id, vertices={}):
        """The constructor for the Graph object. Initializes all graph data.

        @param transaction_id: The system provided transaction id number.
        @param vertices: The system provided transaction id number.
        """
        self.vertices = vertices
        self._creation_transaction_id = transaction_id

    @ray.method(num_return_vals=0)
    def insert(self, key, oid, local_edges, foreign_edges, transaction_id):
        """Inserts the data for a node into the graph.

        @param key: The unique identifier of the node in the graph.
        @param oid: The Ray ObjectID for the Node object referenced by key.
        @param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs.
        @param foreign_edges: A dictionary of edges between graphs.
        @param transaction_id: The system provided transaction id number.
        """
        if type(foreign_edges) is not dict:
            raise ValueError(
                "Foreign edges require destination graph to be specified.")

        if key not in self.vertices:
            self.vertices[key] = \
                [_Vertex(oid, local_edges, foreign_edges, transaction_id)]
        else:
            temp_vertex = self.vertices[key][-1] \
                .copy(oid=oid, transaction_id=transaction_id)
            temp_vertex = temp_vertex.add_local_edges(
                transaction_id, local_edges)
            temp_vertex = temp_vertex.add_foreign_edges(
                transaction_id, foreign_edges)
            self.vertices[key].append(temp_vertex)

    @ray.method(num_return_vals=0)
    def update(self, key, node, local_edges, foreign_edges, transaction_id):
        """Updates the data for a node in the graph.

        @param key: The unique identifier of the node in the graph.
        @param node: The Ray ObjectID for the Node object referenced by key.
        @param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs.
        @param foreign_edges: A dictionary of edges between graphs.
        @param transaction_id: The system provided transaction id number.
        """
        assert self.vertex_exists(key, transaction_id), "Key does not exist"

        last_node = self.vertices[key][-1]
        node = last_node.copy(node, local_edges, foreign_edges, transaction_id)
        self._create_or_update_vertex(key, node)

    @ray.method(num_return_vals=0)
    def delete(self, key, transaction_id):
        """Deletes the data for a node in the graph.

        @param key: The unique identifier of the node in the graph.
        @param transaction_id: The transaction_id for this update.
        """
        self._create_or_update_vertex(key, _DeletedVertex(transaction_id))

    def _create_or_update_vertex(self, key, graph_vertex):
        """Creates or updates the vertex with the key provided.

        @param key: The unique identifier of the node in the graph.
        @param graph_vertex: The vertex to be created/updated.
        """
        if key not in self.vertices:
            self.vertices[key] = [graph_vertex]
        elif graph_vertex._transaction_id == \
                self.vertices[key][-1]._transaction_id:
            # reassignment here because this is an update from within the
            # same transaction
            self.vertices[key][-1] = graph_vertex
        elif graph_vertex._transaction_id > \
                self.vertices[key][-1]._transaction_id:
            self.vertices[key].append(graph_vertex)
        else:
            raise ValueError("Transactions arrived out of order.")

    @ray.method(num_return_vals=0)
    def add_local_edges(self, transaction_id, key, *local_edges):
        """Adds one or more local edges.

        @param transaction_id: The system provided transaction id number.
        @param key: The unique identifier of the node in the graph.
        @param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs.
        """
        if key not in self.vertices:
            graph_vertex = _Vertex().add_local_edges(
                transaction_id, *local_edges)
        else:
            graph_vertex = self.vertices[key][-1].add_local_edges(
                transaction_id, *local_edges)

        self._create_or_update_vertex(key, graph_vertex)

    @ray.method(num_return_vals=0)
    def add_foreign_edges(self, transaction_id, key, graph_id, *foreign_edges):
        """Adds one of more foreign edges.

        @param transaction_id: The system provided transaction id number.
        @param key: The unique identifier of the node in the graph.
        @param graph_id: The unique name of the graph.
        @param foreign_edges: A dictionary of edges between graphs.
        """
        if key not in self.vertices:
            graph_vertex = _Vertex().add_foreign_edges(
                transaction_id, {graph_id: list(foreign_edges)})
        else:
            graph_vertex = \
                self.vertices[key][-1].add_foreign_edges(
                    transaction_id, {graph_id: list(foreign_edges)})

        self._create_or_update_vertex(key, graph_vertex)

    def vertex_exists(self, key, transaction_id):
        """True if the node existed at the time provided, False otherwise.

        @param key: The unique identifier of the node in the graph.
        @param transaction_id: The system provided transaction id number.

        @return: If node exists in graph, returns true, otherwise false.
        """
        return key in self.vertices and \
            self._get_history(transaction_id, key).node_exists()

    def select_vertex(self, transaction_id, key=None):
        """Selects the vertex with the key given at the time given.

        @param transaction_id: The system provided transaction id number.
        @param key: The unique identifier of the node in the graph.

        @return: the requested vertex.
        """
        return [self.select(transaction_id, "oid", key)]

    @ray.method(num_return_vals=2)
    def select_local_edges(self, transaction_id, key=None):
        """Gets the local edges for the key and time provided.

        @param transaction_id: The system provided transaction id number.
        @param key: The unique identifier of the node in the graph.

        @return: the Object ID(s) of the requested local edges.
        """
        edges = self.select(transaction_id, "local_edges", key)
        return edges.edges, edges.buf

    def select_foreign_edges(self, transaction_id, key=None):
        """Gets the foreign keys for the key and time provided.

        @param transaction_id: The system provided transaction id number.
        @param key: The unique identifier of the node in the graph.

        @return: The Object ID(s) of the requested foreign edges.
        """
        return [self.select(transaction_id, "foreign_edges", key)]

    def select(self, transaction_id, prop, key=None):
        """Selects the property given at the time given.

        @param transaction_id: The system provided transaction id number.
        @param prop: The property to be selected.
        @param key: The unique identifier of the node in the graph.

        @return: If no key is provided, returns all vertices from the selected
                 graph.
        """
        if key is None:
            vertices = {}
            for key in self.vertices:
                vertex_at_time = self._get_history(transaction_id, key)
                if prop != "oid" or vertex_at_time.node_exists():
                    vertices[key] = getattr(vertex_at_time, prop)
            return vertices
        else:
            if key not in self.vertices:
                raise ValueError("Key Error. vertex does not exist.")

            obj = self._get_history(transaction_id, key)
            return getattr(obj, prop)

    def _get_history(self, transaction_id, key):
        """Gets the historical state of the object with the key provided.

        @param transaction_id: The system provided transaction id number.
        @param key: The unique identifier of the node in the graph.

        @return: The most recent vertex not exceeding the bounds.
        """
        filtered = list(filter(lambda p: p._transaction_id <= transaction_id,
                               self.vertices[key]))
        if len(filtered) > 0:
            return filtered[-1]
        else:
            return _Vertex()

    def split(self):
        """Splits the graph into two graphs and returns the new graph.
        Note: This modifies the existing Graph also.

        @return: A new Graph with half of the vertices.
        """
        half = int(len(self.vertices)/2)
        items = list(self.vertices.items())
        items.sort()
        second_dict = dict(items[half:])
        self.vertices = dict(items[:half])
        return second_dict

    def getattr(self, item):
        """Gets the attribute.

        @param item: The attribute to be searched for. Must be a string.

        @return: The attribute requested.
        """
        return getattr(self, item)


class _Vertex(object):
    def __init__(self,
                 oid=None,
                 local_edges=[],
                 foreign_edges={},
                 transaction_id=-1):
        """Contains all data for a vertex of the Graph Database.

        @param oid: The Ray ObjectID for the Node object referenced by key.
        @param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs.
        @param foreign_edges: A dictionary of edges between graphs.
        @param transaction_id: The transaction_id that generated this vertex.
        """
        # The only thing we keep as its actual value is the None to filter
        if oid is not None:
            # We need to put it in the Ray store if it's not there already.
            if type(oid) is not ray.local_scheduler.ObjectID:
                self.oid = ray.put(oid)
            else:
                self.oid = oid
        else:
            self.oid = oid

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
             oid=None,
             local_edges=None,
             foreign_edges=None,
             transaction_id=None):
        """Create a copy of this object and replace the provided fields.

        @param oid: the Ray ObjectID for the Node object referenced by key.
        @param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs.
        @param foreign_edges: A dictionary of edges between graphs.
        @param transaction_id: The system provided transaction id number.

        @return: A new _Vertex object containing the copy.
        """
        if oid is None:
            oid = self.oid
        if local_edges is None:
            local_edges = self.local_edges
        if foreign_edges is None:
            foreign_edges = self.foreign_edges
        if transaction_id is None:
            transaction_id = self._transaction_id

        return _Vertex(oid, local_edges, foreign_edges, transaction_id)

    def node_exists(self):
        """Determine if a node exists.

        @return: True if oid is not None, false otherwise.
        """
        return self.oid is not None


class _DeletedVertex(_Vertex):
    def __init__(self, transaction_id):
        """Contains all data for a deleted vertex.

        @param transaction_id: The transaction ID for deleting the vertex.
        """
        super(_DeletedVertex, self).__init__(transaction_id=transaction_id)
