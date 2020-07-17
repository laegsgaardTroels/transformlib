from powertools import discover_pipeline
from squares_with_dag import transforms

pipeline = discover_pipeline(transforms)
