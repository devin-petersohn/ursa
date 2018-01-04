class Edge:
    """
    This object is an edge, or connection, between Nodes in a Graph.

    Fields:
    destination -- the destination key of the connection.
    weight -- a value to represent the strength of the connection. If the
              connection is unweighted, choose weight = 0 (the default).
    orientation -- the direction of the connection. If orientation is
                   irrelevent, choose orientation = none (the default).
    """
    def __init__(self, destination, weight=0, orientation="none"):
        """
        The constructor for an Edge object.

        Keyword arguments:
        destination -- the destination key of the connection.
        weight -- a value to represent the strength of the connection. If the
                  connection is unweighted, choose weight = 0 (the default).
        orientation -- the direction of the connection. If orientation is
                       irrelevent, choose orientation = none (the default).
        """
        self.destination = destination
        self.weight = weight
        self.orientation = orientation

    def update_weight(self, new_weight):
        """
        Updates the weight in this Edge to the value provided.

        Keyword arguments:
        new_weight -- the new weight for this Edge.
        """
        self.weight = new_weight

    def add_to_weight(self, weight_to_add):
        """
        Adds a value to the existing weight and updates it.

        Keyword arguments:
        weight_to_add -- the weight to add to the weight for this Edge.
        """
        self.weight += weight_to_add

    def update_orientation(self, new_orientation):
        """
        Updates the orientation of this Edge to the value provided.

        Keyword arguments:
        new_orientation -- the value to replace the orientation in this Edge.
        """
        self.orientation = new_orientation
