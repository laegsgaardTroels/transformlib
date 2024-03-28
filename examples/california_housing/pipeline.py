from pathlib import Path
import logging
from transformlib import config, Pipeline

here = Path(__file__).parent
pipeline = Pipeline.from_paths(paths=list(here.glob("transforms/*.py")))

if __name__ == "__main__":
    config["data_dir"] = "./data/"
    logging.basicConfig(level=logging.INFO)
    pipeline.run()
