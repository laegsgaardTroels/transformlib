from transformlib import Node


def test_transform_set():
    """Test that running set() on a list of nodes removes duplicates."""
    node = Node('test')
    assert len(set([node, node])) == 1
