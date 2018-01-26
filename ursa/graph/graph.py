import ray


@ray.remote(num_cpus=2)
class Graph(object):
    """This object contains reference and connection information for a graph.

    Fields:
    rows -- The dictionary of _Vertex objects.
    """

    def __init__(self, transaction_id, rows={}):
        """The constructor for the Graph object. Initializes all graph data.

        :param transaction_id:
        :param rows: The system provided transaction id number
        """

        self.rows = rows
        self._creation_transaction_id = transaction_id

    @ray.method(num_return_vals=0)
    def insert(self, key, oid, local_edges, foreign_edges, transaction_id):
        """Inserts the data for a node into the graph

        :param key: the unique identifier of the node in the graph
        :param oid: the Ray ObjectID for the Node object referenced by key
        :param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs
        :param foreign_edges: A dictionary of edges between graphs
        :param transaction_id: The system provided transaction id number
        """

        if type(foreign_edges) is not dict:
            raise ValueError(
                "Foreign edges require destination graph to be specified.")

        if key not in self.rows:
            self.rows[key] = \
                [_Vertex(oid, local_edges, foreign_edges, transaction_id)]
        else:
            temp_row = self.rows[key][-1].copy(oid=oid,
                                               transaction_id=transaction_id)
            temp_row = temp_row.add_local_edges(
                transaction_id, local_edges)
            temp_row = temp_row.add_foreign_edges(
                transaction_id, foreign_edges)
            self.rows[key].append(temp_row)

    @ray.method(num_return_vals=0)
    def update(self, key, node, local_edges, foreign_edges, transaction_id):
        """Updates the data for a node in the graph

        :param key: the unique identifier of the node in the graph
        :param node: the Ray ObjectID for the Node object referenced by key
        :param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs
        :param foreign_edges: A dictionary of edges between graphs
        :param transaction_id: The system provided transaction id number
        """

        assert self.row_exists(key, transaction_id), "Key does not exist"

        last_node = self.rows[key][-1]
        node = last_node.copy(node, local_edges, foreign_edges, transaction_id)
        self._create_or_update_row(key, node)

    @ray.method(num_return_vals=0)
    def delete(self, key, transaction_id):
        """Deletes the data for a node in the graph

        :param key: the unique identifier of the node in the graph
        :param transaction_id: the transaction_id for this update
        """

        self._create_or_update_row(key, _DeletedVertex(transaction_id))

    def _create_or_update_row(self, key, graph_row):
        """Creates or updates the row with the key provided

        :param key: the unique identifier of the node in the graph
        :param graph_row: The row to be created/updated
        """

        if key not in self.rows:
            self.rows[key] = [graph_row]
        elif graph_row._transaction_id == self.rows[key][-1]._transaction_id:
            # reassignment here because this is an update from within the
            # same transaction
            self.rows[key][-1] = graph_row
        elif graph_row._transaction_id > self.rows[key][-1]._transaction_id:
            self.rows[key].append(graph_row)
        else:
            raise ValueError("Transactions arrived out of order.")

    @ray.method(num_return_vals=0)
    def add_local_edges(self, transaction_id, key, *local_edges):
        """Adds one or more local keys

        :param transaction_id: The system provided transaction id number
        :param key: the unique identifier of the node in the graph
        :param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs
        """

        if key not in self.rows:
            graph_row = _Vertex().add_local_edges(
                transaction_id, *local_edges)
        else:
            graph_row = self.rows[key][-1].add_local_edges(
                transaction_id, *local_edges)

        self._create_or_update_row(key, graph_row)

    @ray.method(num_return_vals=0)
    def add_foreign_edges(self, transaction_id, key, graph_id, *foreign_edges):
        """Adds one of more foreign keys

        :param transaction_id: The system provided transaction id number
        :param key: the unique identifier of the node in the graph
        :param graph_id: the unique name of the graph
        :param foreign_edges: A dictionary of edges between graphs
        """

        if key not in self.rows:
            graph_row = _Vertex().add_foreign_edges(
                transaction_id, {graph_id: list(foreign_edges)})
        else:
            graph_row = \
                self.rows[key][-1].add_foreign_edges(
                    transaction_id, {graph_id: list(foreign_edges)})

        self._create_or_update_row(key, graph_row)

    def row_exists(self, key, transaction_id):
        """True if the node existed at the time provided, False otherwise

        :param key: the unique identifier of the node in the graph
        :param transaction_id: The system provided transaction id number

        :return: If node exists in graph, returns true, otherwise false
        """

        return key in self.rows and \
            self._get_history(transaction_id, key).node_exists()

    def select_row(self, transaction_id, key=None):
        """Selects the row with the key given at the time given

        :param transaction_id: The system provided transaction id number
        :param key: the unique identifier of the node in the graph

        :return: the requested row
        """

        return [self.select(transaction_id, "oid", key)]

    def select_local_edges(self, transaction_id, key=None):
        """Gets the local keys for the key and time provided

        :param transaction_id: The system provided transaction id number
        :param key: the unique identifier of the node in the graph

        :return: the Object ID(s) of the requested local edges
        """

        return [self.select(transaction_id, "local_edges", key)]

    def select_foreign_edges(self, transaction_id, key=None):
        """Gets the foreign keys for the key and time provided

        :param transaction_id: The system provided transaction id number
        :param key: the unique identifier of the node in the graph

        :return: the Object ID(s) of the requested foreign edges
        """

        return [self.select(transaction_id, "foreign_edges", key)]

    def select(self, transaction_id, prop, key=None):
        """Selects the property given at the time given

        :param transaction_id: The system provided transaction id number
        :param prop: the property to be selected
        :param key: the unique identifier of the node in the graph

        :return: If no key is provided, returns all rows from the selected
                 graph
        """

        if key is None:
            rows = {}
            for key in self.rows:
                row_at_time = self._get_history(transaction_id, key)
                if prop != "oid" or row_at_time.node_exists():
                    rows[key] = getattr(row_at_time, prop)
            return rows
        else:
            if key not in self.rows:
                raise ValueError("Key Error. Row does not exist.")

            obj = self._get_history(transaction_id, key)
            return getattr(obj, prop)

    def _get_history(self, transaction_id, key):
        """Gets the historical state of the object with the key provided
        :param transaction_id:
        :param key: the unique identifier of the node in the graph

        :return: The most recent vertex not exceeding the bounds
        """

        filtered = list(filter(lambda p: p._transaction_id <= transaction_id,
                               self.rows[key]))
        if len(filtered) > 0:
            return filtered[-1]
        else:
            return _Vertex()

    def split(self):
        """Splits the graph into two graphs and returns the new graph.
        Note: This modifies the existing Graph also.

        :return:A new Graph with half of the rows
        """

        half = int(len(self.rows)/2)
        items = list(self.rows.items())
        items.sort()
        second_dict = dict(items[half:])
        self.rows = dict(items[:half])
        return second_dict

    def getattr(self, item):
        """Gets the attribute

        :param item:

        :return: The attribute requested
        """

        return getattr(self, item)

    def connected_components(self):
        """Gets the connected components

        :return: The set of connected components
        """

        return [_connected_components.remote(self.rows)]


class _Vertex(object):
    def __init__(self,
                 oid=None,
                 local_edges=set(),
                 foreign_edges={},
                 transaction_id=-1):
        """Contains all data for a row of the Graph Database

        :param oid: the Ray ObjectID for the Node object referenced by key
        :param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs
        :param foreign_edges: A dictionary of edges between graphs
        :param transaction_id: The transaction_id that generated this row
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

        # Sometimes we get data that is already in the Ray store e.g. copy()
        # and sometimes we get data that is not e.g. insert()
        # We have to do a bit of work to ensure that our invariants are met.
        if type(local_edges) is not ray.local_scheduler.ObjectID:
            try:
                self.local_edges = ray.put(set([local_edges]))
            except TypeError:
                self.local_edges = ray.put(set(local_edges))
        else:
            self.local_edges = local_edges

        for key in foreign_edges:
            if type(foreign_edges[key]) is not ray.local_scheduler.ObjectID:
                try:
                    foreign_edges[key] = ray.put(set([foreign_edges[key]]))
                except TypeError:
                    foreign_edges[key] = ray.put(set(foreign_edges[key]))

        self.foreign_edges = foreign_edges
        self._transaction_id = transaction_id

    def filter_local_edges(self, filterfn, transaction_id):
        """Filter the local keys based on the provided filter function

        :param filterfn: The function to use to filter the keys
        :param transaction_id: The system provdided transaction id number

        :return:  A new _Vertex object containing the filtered keys
        """

        assert transaction_id >= self._transaction_id, \
            "Transactions arrived out of order."

        return self.copy(local_edges=_apply_filter.remote(
            filterfn, self.local_edges), transaction_id=transaction_id)

    def filter_foreign_edges(self, filterfn, transaction_id, *graph_ids):
        """Filter the foreign keys keys based on the provided filter function

        :param filterfn: The function to use to filter the keys
        :param transaction_id: The system provdided transaction id number
        :param graph_ids: One or more graph ids to apply the filter to

        :return: A new _Vertex object containing the filtered keys
        """

        assert transaction_id >= self._transaction_id, \
            "Transactions arrived out of order."

        if transaction_id > self._transaction_id:
            # we are copying for the new transaction id so that we do not
            # overwrite our previous history.
            new_keys = self.foreign_edges.copy()
        else:
            new_keys = self.foreign_edges

        for graph_id in graph_ids:
            new_keys[graph_id] = _apply_filter.remote(filterfn,
                                                      new_keys[graph_id])

        return self.copy(foreign_edges=new_keys, transaction_id=transaction_id)

    def add_local_edges(self, transaction_id, *values):
        """Append to the local keys based on the provided

        :param transaction_id: The system provided transaction id number
        :param values: One or more values to append to the local keys

        :return: A new _Vertex object containing the appended keys
        """

        assert transaction_id >= self._transaction_id,\
            "Transactions arrived out of order."

        return self.copy(local_edges=_apply_append.remote(
            self.local_edges, values),
            transaction_id=transaction_id)

    def add_foreign_edges(self, transaction_id, values):
        """Append to the local keys based on the provided

        :param transaction_id: The system provided transaction id number
        :param values: A dict of {graph_id: set(keys)}

        :return: A new _Vertex object containing the appended keys
        """

        assert transaction_id >= self._transaction_id, \
            "Transactions arrived out of order."
        assert type(values) is dict, \
            "Foreign keys must be dicts: {destination_graph: key}"

        if transaction_id > self._transaction_id:
            # we are copying for the new transaction id so that we do not
            # overwrite our previous history.
            new_keys = self.foreign_edges.copy()
        else:
            new_keys = self.foreign_edges

        for graph_id in values:
            if graph_id not in new_keys:
                new_keys[graph_id] = values[graph_id]
            else:
                new_keys[graph_id] = _apply_append.remote(new_keys[graph_id],
                                                          values[graph_id])

        return self.copy(foreign_edges=new_keys, transaction_id=transaction_id)

    def copy(self,
             oid=None,
             local_edges=None,
             foreign_edges=None,
             transaction_id=None):
        """Create a copy of this object and replace the provided fields

        :param oid: the Ray ObjectID for the Node object referenced by key
        :param local_edges: Edges within the same graph. This is a set of ray
                            ObjectIDs
        :param foreign_edges: A dictionary of edges between graphs
        :param transaction_id: The system provided transaction id number

        :return: A new _Vertex object containing the copy
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
        """Determine if a node exists

        :return: True if oid is not None, false otherwise
        """

        return self.oid is not None


class _DeletedVertex(_Vertex):
    def __init__(self, transaction_id):
        """Contains all data for a deleted row

        :param transaction_id: The transactions ID for deleting the row
        """
        super(_DeletedVertex, self).__init__(transaction_id=transaction_id)


@ray.remote
def _apply_filter(filterfn, obj_to_filter):
    """Apply a filter function to a specified object

    :param filterfn: The function to use to filter the keys
    :param obj_to_filter: The object to apply the filter to

    :return: An object with the applied filter
    """
    return set(filter(filterfn, obj_to_filter))


@ray.remote
def _apply_append(collection, values):
    """Updates the collection with the provided values

    :param collection: The collection to be updated
    :param values: The updated values

    :return: The updated collection
    """
    try:
        collection.update(values)
        return collection
    except TypeError:
        for val in values:
            collection.update(val)

        return collection


@ray.remote
def _connected_components(adj_list):
    """Gets the connected components

    :param adj_list: The adjacency list to be evaluated

    :return: The set of connected components within the adjacency list
    """
    s = {}
    c = []
    for key in adj_list:
        s[key] = _apply_filter.remote(
            lambda row: row > key, adj_list[key][-1].local_edges)

        if ray.get(_all.remote(key, adj_list[key][-1].local_edges)):
            c.append(key)
    return [_get_children.remote(key, s) for key in c]


@ray.remote
def _all(key, adj_list):
    """Checks if all elements in the adjacency list are greater then the
       provided key

    :param key: the unique identifier of the node in the graph
    :param adj_list: The adjacency list to be evaluated

    :return: True if all elements in the adjacency list are greater than the
             key, false if not
    """
    return all(i > key for i in adj_list)


@ray.remote
def _get_children(key, s):
    """Gets all the children of the graph

    :param key: the unique identifier of the node in the graph
    :param s: the adjacency list for a graph

    :return: all children of the graph
    """
    c = ray.get(s[key])
    try:
        res = ray.get([_get_children.remote(i, s) for i in c])
        c.update([j for i in res for j in i])
    except TypeError as e:
        print(e)
    c.add(key)
    return set(c)
