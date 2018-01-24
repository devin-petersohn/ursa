import ray
from .vertex import _Vertex
from .vertex import _DeletedVertex


@ray.remote(num_cpus=2)
class Graph(object):
    """This object contains reference and connection information for a graph.

    @field vertices: The dictionary of _Vertex objects.
    @versions_to_store: The number of versions to keep in memory for each node
    """
    def __init__(self, transaction_id, versions_to_store=5, vertices={}):
        """The constructor for the Graph object. Initializes all graph data.

        @param transaction_id: The system provided transaction id number.
        @param vertices: The system provided transaction id number.
        """
        self.vertices = vertices
        self._creation_transaction_id = transaction_id
        self._versions_to_store = versions_to_store

    @ray.method(num_return_vals=0)
    def insert(self, key, vertex_data, local_edges, foreign_edges,
               transaction_id):
        """Inserts the data for a vertex into the graph.

        @param key: The unique identifier of the vertex in the graph.
        @param vertex_data: The metadata for this vertex.
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
                [_Vertex(vertex_data, local_edges, foreign_edges,
                         transaction_id)]
        else:
            temp_vertex = self.vertices[key][-1] \
                .copy(vertex_data=vertex_data, transaction_id=transaction_id)
            temp_vertex = temp_vertex.add_local_edges(
                transaction_id, local_edges)
            temp_vertex = temp_vertex.add_foreign_edges(
                transaction_id, foreign_edges)
            self.vertices[key].append(temp_vertex)

    @ray.method(num_return_vals=0)
    def update(self, key, vertex_data, local_edges, foreign_edges,
               transaction_id):
        """Updates the data for a vertex in the graph.

        @param key: The unique identifier of the vertex in the graph.
        @param vertex_data: The metadata for this vertex.
        @param local_edges: The list of edges from this vertex within the
                            graph.
        @param foreign_edges: A dictionary of edges between graphs.
        @param transaction_id: The system provided transaction id number.
        """
        assert self.vertex_exists(key, transaction_id), "Key does not exist"

        last_vertex = self.vertices[key][-1]
        vertex = last_vertex.copy(vertex_data, local_edges, foreign_edges,
                                  transaction_id)
        self._create_or_update_vertex(key, vertex)

    @ray.method(num_return_vals=0)
    def delete(self, key, transaction_id):
        """Deletes the data for a vertex in the graph.

        @param key: The unique identifier of the vertex in the graph.
        @param transaction_id: The transaction_id for this update.
        """
        self._create_or_update_vertex(key, _DeletedVertex(transaction_id))

    def _create_or_update_vertex(self, key, graph_vertex):
        """Creates or updates the vertex with the key provided.

        @param key: The unique identifier of the vertex in the graph.
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
        @param key: The unique identifier of the vertex in the graph.
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
        @param key: The unique identifier of the vertex in the graph.
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
        """True if the vertex existed at the time provided, False otherwise.

        @param key: The unique identifier of the vertex in the graph.
        @param transaction_id: The system provided transaction id number.

        @return: If vertex exists in graph, returns true, otherwise false.
        """
        return key in self.vertices and \
            self._get_history(transaction_id, key).vertex_exists()

    def select_vertex(self, transaction_id, key=None):
        """Selects the vertex with the key given at the time given.

        @param transaction_id: The system provided transaction id number.
        @param key: The unique identifier of the vertex in the graph.

        @return: the requested vertex.
        """
        return [self.select(transaction_id, "vertex_data", key)]

    @ray.method(num_return_vals=2)
    def select_local_edges(self, transaction_id, key=None):
        """Gets the local edges for the key and time provided.

        @param transaction_id: The system provided transaction id number.
        @param key: The unique identifier of the vertex in the graph.

        @return: the Object ID(s) of the requested local edges.
        """
        edges = self.select(transaction_id, "local_edges", key)
        return edges.edges, edges.buf

    def select_foreign_edges(self, transaction_id, key=None):
        """Gets the foreign keys for the key and time provided.

        @param transaction_id: The system provided transaction id number.
        @param key: The unique identifier of the vertex in the graph.

        @return: The Object ID(s) of the requested foreign edges.
        """
        return [self.select(transaction_id, "foreign_edges", key)]

    def select(self, transaction_id, prop, key=None):
        """Selects the property given at the time given.

        @param transaction_id: The system provided transaction id number.
        @param prop: The property to be selected.
        @param key: The unique identifier of the vertex in the graph.

        @return: If no key is provided, returns all vertices from the selected
                 graph.
        """
        if key is None:
            vertices = {}
            for key in self.vertices:
                vertex_at_time = self._get_history(transaction_id, key)
                if prop != "vertex_data" or vertex_at_time.vertex_exists():
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
        @param key: The unique identifier of the vertex in the graph.

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
