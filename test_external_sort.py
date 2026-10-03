import os
from external_sort import external_sort

def test_sorts_correctly_with_quotes_and_header(tmp_path):
    src = str(tmp_path / "sample.csv")
    with open(src, "w") as f:
        f.write("id,name,score\n")
        f.write('3,"Smith, Bob",90\n')
        f.write('1,"Doe, John",85\n')
        f.write('2,"Adams, Abigail",95\n')

    external_sort(src, sort_keys=[0], n_proc=2, max_size=0.0001)

    out = str(tmp_path / "sample_sorted.csv")
    with open(out) as f:
        lines = [line.strip() for line in f]

    assert lines == [
        "id,name,score",
        '1,"Doe, John",85',
        '2,"Adams, Abigail",95',
        '3,"Smith, Bob",90',
    ]

def test_multi_key_sort(tmp_path):
    src = str(tmp_path / "multi.csv")
    with open(src, "w") as f:
        f.write("dept,rank,name\n")
        f.write("eng,2,bob\n")
        f.write("eng,1,alice\n")
        f.write("ops,1,charlie\n")

    external_sort(src, sort_keys=[0, 1], n_proc=2, max_size=0.0001, overwrite=True)

    with open(src) as f:
        lines = [line.strip() for line in f]

    assert lines == [
        "dept,rank,name",
        "eng,1,alice",
        "eng,2,bob",
        "ops,1,charlie",
    ]

if __name__ == "__main__":
    import tempfile, pathlib
    with tempfile.TemporaryDirectory() as td:
        p = pathlib.Path(td)
        test_sorts_correctly_with_quotes_and_header(p)
        test_multi_key_sort(p)
    print("All tests passed.")
