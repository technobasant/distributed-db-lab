import csv

from tools.bench import percentiles, summarize


def test_percentiles_on_known_values():
    p = percentiles([float(x) for x in range(1, 101)])
    assert p["n"] == 100 and p["min"] == 1 and p["max"] == 100
    assert p["p50"] == 50.5
    assert percentiles([]) == {"n": 0}


def test_summarize_splits_windows_and_counts_errors(tmp_path):
    path = tmp_path / "load.csv"
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["wall", "op", "ms", "ok", "error"])
        for t in range(0, 30):
            ok = 0 if t == 15 else 1
            w.writerow(
                [
                    1000 + t,
                    "read" if t % 2 else "write",
                    2.0 + t,
                    ok,
                    "" if ok else "OperationalError: x",
                ]
            )
    out = summarize(path, {"start": 1010, "end": 1019})
    assert out["before"]["read_ms"]["n"] + out["before"]["write_ms"]["n"] == 10
    assert out["during"]["errors"] == {"OperationalError: x": 1}
    assert out["after"]["read_ms"]["n"] + out["after"]["write_ms"]["n"] == 10
