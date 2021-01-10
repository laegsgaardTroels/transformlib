from transformlib.testing import ReusedPySparkTestCase
from transformlib import Node


class TestNode(ReusedPySparkTestCase):
    """Used to test the Node class."""

    def test_transform_set(self):
        """Test that running set() on a list of nodes removes duplicates."""
        node = Node('test')
        assert len(set([node, node])) == 1
