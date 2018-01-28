import ray
import numpy as np


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
