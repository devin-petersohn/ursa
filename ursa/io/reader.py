import csv


class reader(object):
    """
    This object constructs a graph from various file types
    """

    def __init__(self, graph_manager):
        """The constructor for the Reader object

        Keyword arguments:
        graph_manager -- manager for this instance
        """

        self.graph_manager = graph_manager

    def graph_from_csv(self, graph_id, file_path, with_header=True):
        """Generates a graph from a CSV file of the header format
        key, oid, local_edges, foreign_edges

        Keyword arguments:
        graph_id -- name of the new graph
        file_path -- path to the CSV file with graph data
        """

        # Load raw CSV data into an array to process
        raw_data = []
        with open(file_path) as csv_file:
            graph_reader = csv.reader(csv_file)
            for row in graph_reader:
                raw_data.append(row)

        # If file has header, remove before processing
        if with_header:
            raw_data = raw_data[1:]

        # Initalize the graph
        self.graph_manager.create_graph(graph_id)

        # Add the nodes and edges
        for row in raw_data:
            key = row[0]
            oid = row[1]

            local_edges = set(row[2].split(";")) if row[2] else set()
            foreign_edges = {}  # (TODO) Implement foreign_edges

            self.graph_manager.insert(
                graph_id, key, oid, local_edges, foreign_edges)

        # (TODO) Change return flag
        return 1

    def graph_from_json(self, graph_id, file_path):
        """Generates a graph from a JSON file

        Keyword arguments:
        graph_id -- name of the new graph
        file_path -- path to the JSON file with graph data
        """

        # (TODO) Implement this
        return
