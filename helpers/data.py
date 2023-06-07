import os


def create_data_path(filename):
    return os.path.join(os.path.dirname(__file__), "..", "data", filename)
