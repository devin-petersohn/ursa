import ray

@ray.remote
class Graph(object):
    """
    This object contains reference and connection information for a graph.

    Fields:
    oid_dictionary -- the dictionary mapping the unique identifier of a node to
                      the ObjectID of the Node object stored in the Ray object
                      store.
    adjacency_list -- the dictionary mapping the unique identifier of a node to
                      an ObjectID of the set of connections within the graph.
                      The set of connections is built asynchronously.
    inter_graph_connections -- the dictionary mapping the unique identifier of
                               a node to the ObjectID of the set of connections
                               between graphs. The set of connections between
                               graphs is a dictionary {graph_id -> other_graph_key}
                               and is built asynchronously.
    """
    
    def __init__(self, transaction_id):
        """
        The constructor for the Graph object. Initializes all graph data.
        """
        self.oid_dictionary = {}
        self.adjacency_list = {}
        self.inter_graph_connections = {}
        self._creation_transaction_id = transaction_id
        
    def insert(self, key, oid, adjacency_list, connections_to_other_graphs, transaction_id):
        """
        Inserts the data for a node into the graph.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        oid -- the Ray ObjectID for the Node object referenced by key.
        adjacency_list -- the list of connections within this graph.
        connections_to_other_graphs -- the connections to the other graphs.
        transaction_id -- the transaction_id for this update.
        """
        if type(connections_to_other_graphs) is not dict:
            raise ValueError("Connections to other graphs require destination graph to be specified.")

        historical_obj = _HistoricalObj(ray.put(oid), transaction_id)

        # if we don't have anything for this key yet, add it as a list
        if not key in self.oid_dictionary:
            self.oid_dictionary[key] = [historical_obj]
        # treat this as a database insert, cannot insert with the same key.
        else:
            raise ValueError("Unable to insert, key: " + str(key) + " already exists.")

        self._add_adjacency_info(key, adjacency_list, connections_to_other_graphs, transaction_id)

    def update(self, transaction_id, key, oid = None, adjacency_list_fn = None, adjacency_list_fn_arg = None, connections_to_other_graphs_fn = None, connections_to_other_graphs_fn_arg = None):
        """Update the graph for the key specified.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        oid -- the Ray ObjectID for the Node object referenced by key.
        adjacency_list -- the list of connections within this graph.
        connections_to_other_graphs -- the connections to the other graphs.
        transaction_id -- the transaction_id for this update.
        """
        if type(connections_to_other_graphs) is not dict:
            raise ValueError("Connections to other graphs require destination graph to be specified.")

        if oid is not None:
            historical_obj = _HistoricalObj(ray.put(oid), transaction_id)

            # treat this as a datqabase update, unable to update without existing
            if not key in self.oid_dictionary:
                raise ValueError("Unable to update, key: " + str(key) + " does not yet exist.")
            # since we are keeping everything, just append
            else:
                self.oid_dictionary[key].append(historical_obj)

        if adjacency_list_fn is not None:
            historical_obj = _HistoricalObj(_deploy_adj_fn(adjacency_list_fn, self.adjacency_list[key], adjacency_list_fn_arg), transaction_id)
            if key not in self.adjacency_list:
                self.adjacency_list[key] = [historical_obj]
            else:
                self.adjacency_list[key].append(historical_obj)

        if connections_to_other_graphs_fn is not None:
            historical_obj = _HistoricalObj(_deploy_adj_fn(connections_to_other_graphs_fn, self.inter_graph_connections[key], connections_to_other_graphs_fn_arg), transaction_id)
            if key not in self.inter_graph_connections:
                self.inter_graph_connections[key] = [historical_obj]
            self.inter_graph_connections[key].append(historical_obj, transaction_id)

        # self._add_adjacency_info(key, adjacency_list, connections_to_other_graphs, transaction_id)

    def _add_adjacency_info(self, key, adjacency_list, connections_to_other_graphs, transaction_id):
        
        if not key in self.adjacency_list:
            historical_obj = _HistoricalObj(ray.put(set(adjacency_list)), transaction_id)
            self.adjacency_list[key] = [historical_obj]
        # because we allow links to nodes that don't exist, this is possible
        # also, possible in the case of updates
        else:
            historical_obj = _HistoricalObj(_add_to_adj_list.remote(self.adjacency_list[key], set(adjacency_list)), transaction_id)
            self.adjacency_list[key].append(historical_obj)

        if not key in self.inter_graph_connections:
            self.inter_graph_connections[key] = {}

        # the way that the connections to other graphs are stored is important
        # to maintain asynchrony, we have to store it as a dict of dicts        
        for other_graph_id in connections_to_other_graphs:
            if not other_graph_id in self.inter_graph_connections[key]:
                # works if we have only a single connection
                try:
                    historical_obj = _HistoricalObj(ray.put(set([connections_to_other_graphs[other_graph_id]])), transaction_id)
                # hits here if we were passed a list
                except TypeError:
                    historical_obj = _HistoricalObj(ray.put(set(connections_to_other_graphs[other_graph_id])), transaction_id)

                # for the first time, we start the history
                self.inter_graph_connections[key][other_graph_id] = [historical_obj]
            else:
                historical_obj = _HistoricalObj(_add_to_adj_list.remote(self.inter_graph_connections[key][other_graph_id], connections_to_other_graphs[other_graph_id]), transaction_id)
                self.inter_graph_connections[key][other_graph_id].append(historical_obj)
    
    def add_new_adjacent_node(self, key, adjacent_node_key, transaction_id):
        """
        Adds a new connection to the adjacency_list for the key provided.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        adjacent_node_key -- the unique identifier of the new connection to be
                             added.
        """
        if not key in self.adjacency_list:
            historical_obj = _HistoricalObj(set([adjacent_node_key]), transaction_id)
            self.adjacency_list[key] = [historical_obj]
        else:
            historical_obj = _HistoricalObj(_add_to_adj_list.remote(self.adjacency_list[key], adjacent_node_key), transaction_id)
            self.adjacency_list[key].append(historical_obj)
        
    def add_inter_graph_connection(self, key, other_graph_id, new_connection, transaction_id):
        """
        Adds a single new connection to another graph. Because all connections
        are bi-directed, connections are created from the other graph to this
        one also.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        other_graph_id -- the name of the graph for the new connection.
        new_connection -- the identifier of the node for the new connection.
        """
        if not key in self.inter_graph_connections:
            self.inter_graph_connection[key] = {}

        if not other_graph_id in self.inter_graph_connections[key]:
            historical_obj = _HistoricalObj(ray.put(set([new_connection])), transaction_id)
            self.inter_graph_connections[key][other_graph_id] = [historical_obj]
        else:
            historical_obj = _HistoricalObj(_add_to_adj_list.remote(self.inter_graph_connections[key][other_graph_id], set([new_connection])), transaction_id)
            self.inter_graph_connections[key][other_graph_id].append(historical_obj)

    def add_multiple_inter_graph_connections(self, key, other_graph_id, new_connection_list, transaction_id):
        """
        Adds a multiple new connections to another graph. Because all
        connections are bi-directed, connections are created from the other
        graph to this one also.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        other_graph_id -- the name of the graph for the new connection.
        new_connection_list -- the list of identifiers of the node for the new
                               connection.
        """
        if not key in self.inter_graph_connections:
            self.inter_graph_connections[key] = {}

        if not other_graph_id in self.inter_graph_connections[key]:
            historical_obj = _HistoricalObj(ray.put(set(new_connection_list)), transaction_id)
            self.inter_graph_connections[key][other_graph_id] = [historical_obj]
        else:
            historical_obj = _HistoricalObj(_add_to_adj_list.remote(self.inter_graph_connections[key][other_graph_id], set(new_connection_list)), transaction_id)
            self.inter_graph_connections[key][other_graph_id].append(historical_obj)

    def node_exists(self, key, transaction_id):
        """
        Determines if a node exists in the graph.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.

        Returns:
        If node exists in graph, returns true, otherwise false.
        """
        return key in self.oid_dictionary and len(self._get_historical_obj(transaction_id, self.oid_dictionary[key])) > 0

    def get_oid_dictionary(self, transaction_id, key = ""):
        """
        Gets the ObjectID of the Node requested. If none requested, returns the
        full dictionary.

        Keyword arguments:
        key -- the unique identifier of the node in the graph (default = "").
        """
        if key == "":
            adj = {}
            for l in self.oid_dictionary:
                historical_obj = self._get_historical_obj(transaction_id, self.oid_dictionary[l])
                if len(historical_obj) > 0:
                    adj[l] = historical_obj[0]
            return adj
        else:
            return self._get_historical_obj(transaction_id, self.oid_dictionary[key])
            # return self.oid_dictionary[key]
    
    def get_adjacency_list(self, transaction_id, key = ""):
        """
        Gets the connections within this graph of the Node requested. If none
        requested, returns the full dictionary.

        Keyword arguments:
        key -- the unique identifier of the node in the graph (default = "").
        """
        if key == "":
            adj = {}
            for l in self.adjacency_list:
                historical_obj = self._get_historical_obj(transaction_id, self.adjacency_list[l])
                if len(historical_obj) > 0:
                    adj[l] = historical_obj[0]
            return adj
        else:
            return self._get_historical_obj(transaction_id, self.adjacency_list[key])
        
    def get_inter_graph_connections(self, transaction_id, key = "", other_graph_id = ""):
        """
        Gets the connections to other graphs of the Node requested. If none
        requested, returns the full dictionary.

        Keyword arguments:
        key -- the unique identifier of the node in the graph (default = "").
        """
        if key == "":
            adj = {}
            for i in self.inter_graph_connections:
                for l in self.inter_graph_connections[i]:
                    historical_obj = self._get_historical_obj(transaction_id, self.inter_graph_connections[i][l])
                    if len(historical_obj) > 0:
                        # wait to start building until now to ensure that
                        # there is something to put here
                        if not i in adj:
                            adj[i] = {}
                        adj[i][l] = historical_obj[0]
            return adj
        elif other_graph_id == "":
            adj = {}
            for l in self.inter_graph_connections[key]:
                historical_obj = self._get_historical_obj(transaction_id, self.inter_graph_connections[key][l])
                if len(historical_obj) > 0:
                    adj[l] = historical_obj[0]
            return adj
        else:
            # TODO: Handle Error correctly
            return self._get_historical_obj(transaction_id, self.inter_graph_connections[key][other_graph_id])

    def _get_historical_obj(self, transaction_id, l):
        """Gets the correct object from a list of historical objects based on transaction_id.

        Keyword arguments:
        transaction_id -- the transaction_id to apply.
        l -- the list of historical objects.

        Returns:
        A list containing 0 or 1 elements, based on the transaction_id. If
        there is no record at that particular transaction_id the list will be
        empty. Otherwise we give the highest transaction_id that is less than
        the transaction_id provided, wrapped in a list.
        """
        filtered = list(filter(lambda p: p.transaction_id <= transaction_id, l))
        if len(filtered) > 0:
            return [max(filtered).obj]
        else:
            return filtered

class _HistoricalObj(object):
    """This class serves as the wrapper for historical objects.

    The purpose is to clean up the code and make it simpler to add and delete
    rows in the database.
    """
    def __init__(self, obj, transaction_id):
        self.obj = obj
        self.transaction_id = transaction_id

    def __lt__(self, other):
        return self.transaction_id < other.transaction_id


@ray.remote
def _deploy_adj_fn(fn, adj_list, other):
    if other is not None:
        return fn(adj_list, other)
    else:
        return fn(adj_list)

@ray.remote
def _add_to_adj_list(adj_list, other_key):
    """
    Adds one or multiple keys to the list provided. This can add to both the
    adjacency list and the connections between graphs.

    The need for this stems from trying to make updates to the graph as
    asynchronous as possible.

    Keyword arguments:
    adj_list -- the list of connections to append to.
    other_key -- a set of connections or a single value to add to adj_list.

    Returns:
    The updated list containing the newly added value(s).
    """
    try:
        adj_list.update(set([other_key]))
    except TypeError:
        adj_list.update(set(other_key))
    
    return adj_list
