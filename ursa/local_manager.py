import ray
from . import graph as ug


class Graph_manager(object):
    """
    This object manages all graphs in the system.

    Fields:
    graph_dict -- the dictionary of graphs.
                           The keys for the dictionary are the graph_ids, and
                           the values are the Graph objects.
    """
    def __init__(self):
        """
        The constructor for an Graph_collection object. Initializes some toy
        graphs as an example.
        """
        self.graph_dict = {}
        self._transaction_id = 0

    def update_transaction_id(self):
        """
        Updates the transaction ID with that of the global graph manager.
        """
        pass

    def create_graph(self, graph_id, new_transaction=True):
        """
        Create an empty graph.

        Keyword arguments:
        graph_id -- the name of the new graph.
        """
        if new_transaction:
            self._transaction_id += 1

        if not graph_id or graph_id == "":
            raise ValueError("Graph must be named something.")
        if graph_id in self.graph_dict:
            raise ValueError("Graph name already exists.")
        self.graph_dict[graph_id] = ug.Graph.remote(self._transaction_id)

    def _create_if_not_exists(self, graph_id):
        if graph_id not in self.graph_dict:
            print("Warning:", str(graph_id),
                  "is not yet in this Graph Collection. Creating...")

            self.create_graph(graph_id, new_transaction=False)

    def insert(self, graph_id, key, node, local_edges=set(), foreign_edges={}):
        """
        Adds data to the graph specified.

        Keyword arguments:
        graph_id -- the unique name of the graph.
        key -- the unique identifier of this data in the graph.
        node -- the data to add to the graph.
        local_edges -- A list of edges within this graph, if any.
        foreign_edges -- A dictionary: {graph id: key}
        """
        self._transaction_id += 1

        if type(foreign_edges) is not dict:
            raise ValueError(
                "Foreign keys must be labeled with a destination graph.")

        self._create_if_not_exists(graph_id)

        self.graph_dict[graph_id].insert.remote(key,
                                                node,
                                                local_edges,
                                                foreign_edges,
                                                self._transaction_id)

        try:
            local_edges = set([local_edges])
        except TypeError:
            local_edges = set(local_edges)

        for back_edge_key in local_edges:
            self.graph_dict[graph_id].add_local_edges.remote(
                self._transaction_id, back_edge_key, key)

        for other_graph_id in foreign_edges:
            self._create_if_not_exists(other_graph_id)

            for back_edge_key in foreign_edges:
                self.graph_dict[other_graph_id].add_foreign_edges.remote(
                    self._transaction_id,
                    back_edge_key,
                    graph_id,
                    key)

    def update(self, graph_id, key, node=None, local_edges=None,
               foreign_edges=None):
        """Updates the user specified row and all associated edges
        """
        if node is None and local_edges is None and foreign_edges is None:
            raise ValueError(
                "No values provided to update.")

        self._transaction_id += 1

        self.graph_dict[graph_id].update.remote(key,
                                                node,
                                                local_edges,
                                                foreign_edges,
                                                self._transaction_id)

    def delete_row(self, graph_id, key):
        """Deletes the user specified row and all associated edges
        """
        self._transaction_id += 1

        self.graph_dict[graph_id].delete.remote(key, self._transaction_id)

    def add_local_edges(self, graph_id, key, *local_edges):
        """Adds one or more local keys to the graph and key provided.
        """
        self._transaction_id += 1
        self.graph_dict[graph_id].add_local_edges.remote(self._transaction_id,
                                                        key,
                                                        *local_edges)

        for back_edge_key in local_edges:
            self.graph_dict[graph_id].add_local_edges.remote(
                self._transaction_id, back_edge_key, key)

    def add_foreign_edges(self, graph_id, key, other_graph_id, *foreign_edges):
        """Adds one or more foreign keys to the graph and key provided.
        """
        self._transaction_id += 1

        self.graph_dict[graph_id].add_foreign_edges.remote(
            self._transaction_id,
            key,
            other_graph_id,
            *foreign_edges)

        self._create_if_not_exists(other_graph_id)

        for back_edge_key in foreign_edges:
            self.graph_dict[other_graph_id].add_foreign_edges.remote(
                self._transaction_id,
                back_edge_key,
                graph_id,
                key)

    def node_exists(self, graph_id, key):
        """
        Determines whether or not a node exists in the graph.

        Keyword arguments:
        graph_id -- the unique name of the graph
        key -- the unique identifier of the node in the graph.

        Returns:
        True if both the graph exists and the node exists in the graph,
        false otherwise
        """
        return graph_id in self.graph_dict and \
            self.graph_dict[graph_id].row_exists.remote(key)

    def select_row(self, graph_id, key=None):
        return ray.get(self.graph_dict[graph_id].select_row.remote(
            self._transaction_id, key))[0]

    def select_local_edges(self, graph_id, key=None):
        return ray.get(self.graph_dict[graph_id].select_local_edges.remote(
            self._transaction_id, key))[0]

    def select_foreign_edges(self, graph_id, key=None):
        return ray.get(self.graph_dict[graph_id].select_foreign_edges.remote(
            self._transaction_id, key))[0]

    def get_graph(self, graph_id):
        """
        Gets the graph requested.

        Keyword arguments:
        graph_id -- the unique name of the graph.

        Returns:
        The Graph object for the graph requested.
        """
        return self.graph_dict[graph_id]

    def split_graph(self, graph_id):
        """Splits a graph into two graphs.
        """
        second_split = self.graph_dict[graph_id].split.remote()
        t_id = ray.get(self.graph_dict[graph_id].getattr.remote(
                "_creation_transaction_id"))
        self.graph_dict[graph_id] = \
            [self.graph_dict[graph_id],
             ug.Graph.remote(t_id, second_split)]
