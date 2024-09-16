from pathlib import Path
import logging
from transformlib import Pipeline, configure

configure(data_dir="./data/")
here = Path(__file__).parent
pipeline = Pipeline.from_paths(paths=list(here.glob("transforms/*.py")))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    pipeline.run()
