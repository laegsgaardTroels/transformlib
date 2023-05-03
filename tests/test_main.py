from transformlib.__main__ import main


def test_main(tmp_path, monkeypatch):
    monkeypatch.setenv('TRANSFORMLIB_DATA_DIR', str(tmp_path))
    (tmp_path / 'mapping.txt').write_text("""1,2
3,4
5,6
7,8
9,10""")
    main(path=['tests/transforms/mapping.py'], verbose=True)
    assert (tmp_path / 'mapping.json').exists()
