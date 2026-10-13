import csv
from collections import Counter
from pathlib import Path

from tools.datagen import SAAS_SCALES, TICK_SCALES, gen_events, gen_saas, gen_ticks


def read(path: Path) -> list[dict[str, str]]:
    with path.open() as fh:
        return list(csv.DictReader(fh))


def test_saas_is_byte_identical_for_the_same_seed(tmp_path):
    gen_saas(SAAS_SCALES["tiny"], tmp_path / "a", seed=7)
    gen_saas(SAAS_SCALES["tiny"], tmp_path / "b", seed=7)
    for f in (tmp_path / "a").glob("*.csv"):
        assert f.read_bytes() == (tmp_path / "b" / f.name).read_bytes(), f.name


def test_saas_whale_and_null_tenant_shares(tmp_path):
    scale = SAAS_SCALES["small"]
    gen_saas(scale, tmp_path, seed=7)
    events = read(tmp_path / "events.csv")
    assert len(events) == scale.events
    tenants = Counter(e["tenant_id"] for e in events)
    nulls = tenants.pop("")
    whale = tenants["1"] / len(events)
    assert 0.13 < whale < 0.17
    assert 0.003 < nulls / len(events) < 0.008
    # the whale is the largest tenant, clearly ahead of the next one
    (top, top_n), (_, second_n) = tenants.most_common(2)
    assert top == "1" and top_n >= 1.25 * second_n


def test_events_reference_a_user_of_the_same_tenant(tmp_path):
    gen_saas(SAAS_SCALES["tiny"], tmp_path, seed=7)
    user_tenant = {u["id"]: u["tenant_id"] for u in read(tmp_path / "users.csv")}
    for e in read(tmp_path / "events.csv"):
        if e["user_id"]:
            assert user_tenant[e["user_id"]] == e["tenant_id"] or e["tenant_id"] == ""


def test_audit_log_keeps_its_mixed_case_header(tmp_path):
    gen_saas(SAAS_SCALES["tiny"], tmp_path, seed=7)
    header = (tmp_path / "AuditLog.csv").read_text().splitlines()[0]
    assert header == "Id,TenantId,Action,CreatedAt"


def test_ticks_row_count_matches_the_scale(tmp_path):
    s = TICK_SCALES["tiny"]
    counts = gen_ticks(s, tmp_path, seed=7)
    assert counts["ticks"] == s.entities * s.metrics * (s.days * 86_400 // s.step_s)


def test_events_dataset_shape(tmp_path):
    gen_events(5_000, tmp_path, seed=7)
    rows = read(tmp_path / "events.csv")
    assert len(rows) == 5_000
    purchases = [r for r in rows if r["event_type"] == "purchase"]
    assert purchases and all(float(r["revenue"]) > 0 for r in purchases)
    assert all(float(r["revenue"]) == 0 for r in rows if r["event_type"] != "purchase")
