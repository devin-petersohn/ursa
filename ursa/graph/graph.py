import ray
from .vertex import _Vertex
from .vertex import _DeletedVertex
from .utils import write_row, read_row


@ray.remote(num_cpus=2)
class Graph(object):
    """This object contains reference and connection information for a graph.

    @field vertices: The dictionary of _Vertex objects.
    @field versions_to_store: The number of versions to keep in memory for each
                              node
    @field graph_id: The globally unique ID to identify this graph.
    """
    def __init__(self, transaction_id, versions_to_store=5, vertices={}):
        """The constructor for the Graph object. Initializes all graph data.

        @param transaction_id: The system provided transaction id number.
        @param vertices: The system provided transaction id number.
        """
        self.vertices = vertices
        self._creation_transaction_id = transaction_id
        self._versions_to_store = versions_to_store
        self.graph_id = graph_id

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

    def clean_old_rows(self):
        """Spills all rows older than versions_to_store to disk.
        """
        for k, rows in self.rows.items():
            if len(rows) < self.versions_to_store:
                continue
            self.rows[k] = rows[-self.versions_to_store:]
            rows_to_write = rows[:-self.versions_to_store]
            [write_row(r, self.graph_id, k) for r in rows_to_write]
<<<<<<< HEAD
=======


class _GraphRow(object):
    """Contains all data for a row of the Graph Database.

    Fields:
    oid -- The ray ObjectID for the data in the row.
    local_keys -- Edges within the same graph. This is a set of ray ObjectIDs.
    foreign_keys -- Edges between graphs. This is a dict: {graph_id: ObjectID}.
    _transaction_id -- The transaction_id that generated this row.
    """
    def __init__(self,
                 oid=None,
                 local_keys=set(),
                 foreign_keys={},
                 transaction_id=-1):

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
        if type(local_keys) is not ray.local_scheduler.ObjectID:
            try:
                self.local_keys = ray.put(set([local_keys]))
            except TypeError:
                self.local_keys = ray.put(set(local_keys))
        else:
            self.local_keys = local_keys

        for key in foreign_keys:
            if type(foreign_keys[key]) is not ray.local_scheduler.ObjectID:
                try:
                    foreign_keys[key] = ray.put(set([foreign_keys[key]]))
                except TypeError:
                    foreign_keys[key] = ray.put(set(foreign_keys[key]))

        self.foreign_keys = foreign_keys
        self._transaction_id = transaction_id

    def filter_local_keys(self, filterfn, transaction_id):
        """Filter the local keys based on the provided filter function.

        Keyword arguments:
        filterfn -- The function to use to filter the keys.
        transaction_id -- The system provdided transaction id number.

        Returns:
        A new _GraphRow object containing the filtered keys.
        """
        assert transaction_id >= self._transaction_id, \
            "Transactions arrived out of order."

        return self.copy(local_keys=_apply_filter.remote(filterfn,
                                                         self.local_keys),
                         transaction_id=transaction_id)

    def filter_foreign_keys(self, filterfn, transaction_id, *graph_ids):
        """Filter the foreign keys keys based on the provided filter function.

        Keyword arguments:
        filterfn -- The function to use to filter the keys.
        transaction_id -- The system provdided transaction id number.
        graph_ids -- One or more graph ids to apply the filter to.

        Returns:
        A new _GraphRow object containing the filtered keys.
        """
        assert transaction_id >= self._transaction_id, \
            "Transactions arrived out of order."

        if transaction_id > self._transaction_id:
            # we are copying for the new transaction id so that we do not
            # overwrite our previous history.
            new_keys = self.foreign_keys.copy()
        else:
            new_keys = self.foreign_keys

        for graph_id in graph_ids:
            new_keys[graph_id] = _apply_filter.remote(filterfn,
                                                      new_keys[graph_id])

        return self.copy(foreign_keys=new_keys, transaction_id=transaction_id)

    def add_local_keys(self, transaction_id, *values):
        """Append to the local keys based on the provided.

        Keyword arguments:
        transaction_id -- The system provdided transaction id number.
        values -- One or more values to append to the local keys.

        Returns:
        A new _GraphRow object containing the appended keys.
        """
        assert transaction_id >= self._transaction_id,\
            "Transactions arrived out of order."

        return self.copy(local_keys=_apply_append.remote(self.local_keys,
                                                         values),
                         transaction_id=transaction_id)

    def add_foreign_keys(self, transaction_id, values):
        """Append to the local keys based on the provided.

        Keyword arguments:
        transaction_id -- The system provdided transaction id number.
        values -- A dict of {graph_id: set(keys)}.

        Returns:
        A new _GraphRow object containing the appended keys.
        """
        assert transaction_id >= self._transaction_id, \
            "Transactions arrived out of order."
        assert type(values) is dict, \
            "Foreign keys must be dicts: {destination_graph: key}"

        if transaction_id > self._transaction_id:
            # we are copying for the new transaction id so that we do not
            # overwrite our previous history.
            new_keys = self.foreign_keys.copy()
        else:
            new_keys = self.foreign_keys

        for graph_id in values:
            if graph_id not in new_keys:
                new_keys[graph_id] = values[graph_id]
            else:
                new_keys[graph_id] = _apply_append.remote(new_keys[graph_id],
                                                          values[graph_id])

        return self.copy(foreign_keys=new_keys, transaction_id=transaction_id)

    def copy(self,
             oid=None,
             local_keys=None,
             foreign_keys=None,
             transaction_id=None):
        """Create a copy of this object and replace the provided fields.
        """
        if oid is None:
            oid = self.oid
        if local_keys is None:
            local_keys = self.local_keys
        if foreign_keys is None:
            foreign_keys = self.foreign_keys
        if transaction_id is None:
            transaction_id = self._transaction_id

        return _GraphRow(oid, local_keys, foreign_keys, transaction_id)

    def node_exists(self):
        """True if oid is not None, false otherwise.
        """
        return self.oid is not None


class _DeletedGraphRow(_GraphRow):
    """Contains all data for a deleted row.
    """
    def __init__(self, transaction_id):
        super(_DeletedGraphRow, self).__init__(transaction_id=transaction_id)


@ray.remote
def read_graph_row(file):
    oid, local_keys, foreign_keys = read_row.remote(file)
    return _GraphRow(ray.get(oid), ray.get(local_keys), ray.get(foreign_keys))


@ray.remote
def _apply_filter(filterfn, obj_to_filter):
    return set(filter(filterfn, obj_to_filter))


@ray.remote
def _apply_append(collection, values):
    try:
        collection.update(values)
        return collection
    except TypeError:
        for val in values:
            collection.update(val)

        return collection


@ray.remote
def _connected_components(adj_list):
    s = {}
    c = []
    for key in adj_list:
        s[key] = _apply_filter.remote(
            lambda row: row > key, adj_list[key][-1].local_keys)

        if ray.get(_all.remote(key, adj_list[key][-1].local_keys)):
            c.append(key)
    return [_get_children.remote(key, s) for key in c]


@ray.remote
def _all(key, adj_list):
    return all(i > key for i in adj_list)


@ray.remote
def _get_children(key, s):
    c = ray.get(s[key])
    try:
        res = ray.get([_get_children.remote(i, s) for i in c])
        c.update([j for i in res for j in i])
    except TypeError as e:
        print(e)
    c.add(key)
    return set(c)
>>>>>>> implementing deserialization of graph rows
