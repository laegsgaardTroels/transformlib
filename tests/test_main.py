from transformlib import config
from transformlib.__main__ import main


def test_main(tmp_path, monkeypatch, mapping_txt, mapping_py):
    monkeypatch.setitem(config, "data_dir", str(tmp_path))
    (tmp_path / "mapping.txt").write_text(mapping_txt)
    (tmp_path / "mapping.py").write_text(mapping_py)
    main(paths=[tmp_path / "mapping.py"], verbose=True)
    assert (tmp_path / "mapping.json").exists()
