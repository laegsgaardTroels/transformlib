import pytest


@pytest.fixture
def mapping_txt() -> str:
    return """1,2
3,4
5,6
7,8
9,10"""


@pytest.fixture
def mapping_py() -> str:
    return """import json
from transformlib import transform, Output, Input


@transform(
    json_output=Output('mapping.json'),
    txt_input=Input('mapping.txt'),
)
def convert_to_json(json_output, txt_input):
    text = txt_input.path.read_text()
    mapping = dict(map(lambda line: line.split(','), text.splitlines()))
    json_output.path.write_text(json.dumps(mapping, indent=4))
"""
