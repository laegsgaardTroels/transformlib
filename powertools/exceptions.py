class PowertoolsCycleException(Exception):
    """Raised when there is a cycle in the DAG."""


class PowertoolsDuplicateTransformException(Exception):
    """Raised when there is duplicate transforms in a Pipeline."""
