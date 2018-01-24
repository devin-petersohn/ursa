import ray
from ray import pyarrow as pa
import numpy as np
import os


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


@ray.remote
def write_row(row, graph_id, key):
    dest_directory = graph_id + "/" + key + "/"

    # @TODO(kunalgosar): Accessing _transaction_id here is not recommended
    dest_file = dest_directory + str(row._transaction_id) + ".dat"
    if not os.path.exists(dest_directory):
        os.makedirs(dest_directory)

    oids = [row.oid, row.local_keys]
    foreign_keys = list(row.foreign_keys.keys())
    oids.extend([row.foreign_keys[k] for k in foreign_keys])
    oids = [pa.plasma.ObjectID(oid.id()) for oid in oids]

    buffers = ray.worker.global_worker.plasma_client.get_buffers(oids)
    data = {"node": buffers[0],
            "local_keys": buffers[1],
            "foreign_keys": foreign_keys,
            "foreign_key_values": buffers[2:]}

    serialization_context = ray.worker.global_worker.serialization_context
    serialized = pa.serialize(data, serialization_context).to_buffer()
    with pa.OSFile(dest_file, 'wb') as f:
        f.write(serialized)

    return dest_file


@ray.remote(num_return_vals=3)
def read_row(file):
    mmap = pa.memory_map(file)
    buf = mmap.read()

    serialization_context = ray.worker.global_worker.serialization_context
    data = pa.deserialize(buf, serialization_context)

    oid = pa.deserialize(data["node"], serialization_context)
    local_keys = pa.deserialize(data["local_keys"], serialization_context)
    foreign_key_values = [pa.deserialize(x, serialization_context)
                          for x in data["foreign_key_values"]]
    foreign_keys = dict(zip(data["foreign_keys"], foreign_key_values))
    return oid, local_keys, foreign_keys
