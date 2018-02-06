import ray
from ray import pyarrow as pa
import numpy as np
import os
import ursa


@ray.remote
def _filter_remote(filterfn, chunk):
    """Apply a filter function to an object.

    @param filterfn: The filter function to be applied to the list of
                     edges.
    @param chunk: The object to which the filter will be applied.

    @return: An array of filtered objects.
    """
    return np.array(filter(filterfn, chunk))


@ray.remote
def _apply_filter(filterfn, obj_to_filter):
    """Apply a filter function to a specified object.

    @param filterfn: The function to use to filter the keys.
    @param obj_to_filter: The object to apply the filter to.

    @return: A set that contains the result of the filter.
    """
    return set(filter(filterfn, obj_to_filter))


@ray.remote
def _apply_append(collection, values):
    """Updates the collection with the provided values.

    @param collection: The collection to be updated.
    @param values: The updated values.

    @return: The updated collection.
    """
    try:
        collection.update(values)
        return collection
    except TypeError:
        for val in values:
            collection.update(val)

        return collection


@ray.remote(num_return_vals=0)
def write_vertex(vertex, graph_id, key):
    dest_directory = graph_id + "/" + key + "/"

    # @TODO(kunalgosar): Accessing _transaction_id here is not recommended
    dest_file = dest_directory + str(vertex._transaction_id) + ".dat"
    if not os.path.exists(dest_directory):
        os.makedirs(dest_directory)

    serialization_context = ray.worker.global_worker.serialization_context

    oids = [vertex.vertex_data]
    oids.extend(vertex.local_edges.edges)
    local_edges_buffer = pa.serialize(vertex.local_edges.buf,
                                      serialization_context).to_buffer()

    oids.append(local_edges_buffer)
    num_local_edge_lists = len(vertex.local_edges.edges) + 1

    foreign_edges = list(vertex.foreign_edges.keys())
    oids.extend([vertex.foreign_edges[k] for k in foreign_edges])
    oids = [pa.plasma.ObjectID(oid.id()) for oid in oids
            if type(oid) is ray.local_scheduler.ObjectID]

    buffers = ray.worker.global_worker.plasma_client.get_buffers(oids)
    data = {"vertex_data": buffers[0],
            "local_edges": buffers[1:1 + num_local_edge_lists],
            "foreign_edges": foreign_edges,
            "foreign_edge_values": buffers[1 + num_local_edge_lists:],
            "transaction_id": vertex._transaction_id}

    serialized = pa.serialize(data, serialization_context).to_buffer()
    with pa.OSFile(dest_file, 'wb') as f:
        f.write(serialized)


@ray.remote
def read_vertex(file):
    mmap = pa.memory_map(file)
    buf = mmap.read()

    serialization_context = ray.worker.global_worker.serialization_context
    data = pa.deserialize(buf, serialization_context)

    vertex_data = pa.deserialize(data["vertex_data"], serialization_context)
    local_edges = np.array([pa.deserialize(x, serialization_context)
                            for x in data["local_edges"]]).flatten()

    foreign_edge_values = [pa.deserialize(x, serialization_context)
                           for x in data["foreign_edge_values"]]

    foreign_edges = dict(zip(data["foreign_edges"], foreign_edge_values))

    new_vertex = ursa.graph.vertex._Vertex(vertex_data,
                                           local_edges,
                                           foreign_edges,
                                           data["transaction_id"])

    return new_vertex
