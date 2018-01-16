import ray


@ray.remote(num_cpus=2)
class Graph(object):
    """
    This object contains reference and connection information for a graph.

    Fields:
    rows -- The dictionary of _GraphRow objects.
    """

    def __init__(self, transaction_id, rows={}):
        """The constructor for the Graph object. Initializes all graph data.
        """
        self.rows = rows
        self._creation_transaction_id = transaction_id

    def insert(self, key, oid, local_keys, foreign_keys, transaction_id):
        """Inserts the data for a node into the graph.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        oid -- the Ray ObjectID for the Node object referenced by key.
        local_keys -- the list of connections within this graph.
        foreign_keys -- the connections to the other graphs.
        transaction_id -- the transaction_id for this update.
        """
        if type(foreign_keys) is not dict:
            raise ValueError(
                "Foreign keys require destination graph to be specified.")

        if key not in self.rows:
            self.rows[key] = \
                [_GraphRow(oid, local_keys, foreign_keys, transaction_id)]
        else:
            temp_row = self.rows[key][-1].copy(oid=oid,
                                               transaction_id=transaction_id)
            temp_row = temp_row.add_local_keys(transaction_id, local_keys)
            temp_row = temp_row.add_foreign_keys(transaction_id, foreign_keys)
            self.rows[key].append(temp_row)

    def update(self, key, node, local_keys, foreign_keys, transaction_id):
        """Updates the data for a node in the graph.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        oid -- the Ray ObjectID for the Node object referenced by key.
        local_keys -- the list of connections within this graph.
        foreign_keys -- the connections to the other graphs.
        transaction_id -- the transaction_id for this update.
        """
        assert self.row_exists(key, transaction_id), "Key does not exist"

        last_node = self.rows[key][-1]
        node = last_node.copy(node, local_keys, foreign_keys, transaction_id)
        self._create_or_update_row(key, node)

    def delete(self, key, transaction_id):
        """Deletes the data for a node in the graph.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        oid -- the Ray ObjectID for the Node object referenced by key.
        local_keys -- the list of connections within this graph.
        foreign_keys -- the connections to the other graphs.
        transaction_id -- the transaction_id for this update.
        """
        self._create_or_update_row(key, _DeletedGraphRow(transaction_id))

    def _create_or_update_row(self, key, graph_row):
        """Creates or updates the row with the key provided.
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

    def add_local_keys(self, transaction_id, key, *local_keys):
        """Adds one or more local keys.
        """
        if key not in self.rows:
            graph_row = _GraphRow().add_local_keys(transaction_id, *local_keys)
        else:
            graph_row = self.rows[key][-1].add_local_keys(
                transaction_id, *local_keys)

        self._create_or_update_row(key, graph_row)

    def add_foreign_keys(self, transaction_id, key, graph_id, *foreign_keys):
        """Adds one of more foreign keys.
        """
        if key not in self.rows:
            graph_row = _GraphRow().add_foreign_keys(
                transaction_id, {graph_id: list(foreign_keys)})
        else:
            graph_row = \
                self.rows[key][-1].add_foreign_keys(
                    transaction_id, {graph_id: list(foreign_keys)})

        self._create_or_update_row(key, graph_row)

    def row_exists(self, key, transaction_id):
        """True if the node existed at the time provided, False otherwise.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.

        Returns:
        If node exists in graph, returns true, otherwise false.
        """
        return key in self.rows and \
            self._get_history(transaction_id, key).node_exists()

    def select_row(self, transaction_id, key=None):
        """Selects the row with the key given at the time given
        """
        return [self.select(transaction_id, "oid", key)]

    def select_local_keys(self, transaction_id, key=None):
        """Gets the local keys for the key and time provided.
        """
        return [self.select(transaction_id, "local_keys", key)]

    def select_foreign_keys(self, transaction_id, key=None):
        """Gets the foreign keys for the key and time provided.
        """
        return [self.select(transaction_id, "foreign_keys", key)]

    def select(self, transaction_id, prop, key=None):
        """Selects the property given at the time given.
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
        """Gets the historical state of the object with the key provided.
        """
        filtered = list(filter(lambda p: p._transaction_id <= transaction_id,
                               self.rows[key]))
        if len(filtered) > 0:
            return filtered[-1]
        else:
            return _GraphRow()

    def split(self):
        """Splits the graph into two graphs and returns the new graph.

        Note: This modifies the existing Graph also.

        :return: A new Graph with half of the rows.
        """
        half = int(len(self.rows)/2)
        items = list(self.rows.items())
        items.sort()
        second_dict = dict(items[half:])
        self.rows = dict(items[:half])
        return second_dict

    def getattr(self, item):
        """Gets the attribute.
        """
        return getattr(self, item)

    def connected_components(self):
        """Gets the connected components
        """
        return [_connected_components.remote(self.rows)]


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
