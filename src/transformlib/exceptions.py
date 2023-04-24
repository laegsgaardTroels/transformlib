class TransformlibCycleException(Exception):
    """Raised when there is a cycle in the DAG."""


class TransformlibDuplicateTransformException(Exception):
    """Raised when there is duplicate transforms in a Pipeline."""
