# distributed-db-lab — one part at a time; tear each down before the next.
#
#   make setup          uv venv + dependencies
#   make check          ruff, tests, shellcheck, compose validation
#   make data           generate all three datasets at lab scale (~2 GB of CSV)
#   make citus          Citus: cluster, naive attempt, plan, migrate, queries, rebalance, skew
#   make tsdb           TimescaleDB: keys, chunks, compression, caggs, retention, vacuum
#   make ch             ClickHouse: schema, load, variants, MV, parts, replica failover
#   make compare        same dataset in Postgres, Citus columnar, TimescaleDB, ClickHouse
#   make down           stop and remove every container and volume

SHELL := /usr/bin/env bash
SCALE ?= lab
PY := .venv/bin/python
COMPOSE_FILES := citus/docker-compose.yml timescaledb/docker-compose.yml \
                 clickhouse/docker-compose.yml compare/docker-compose.yml

.PHONY: setup check data data-saas data-ticks data-events citus tsdb ch compare down

setup:
	uv sync

check:
	uv run ruff check tools tests
	uv run ruff format --check tools tests
	uv run pytest -q
	shellcheck */scripts/*.sh
	for f in $(COMPOSE_FILES); do docker compose -f $$f config -q; done

data: data-saas data-ticks data-events
data-saas:   ; $(PY) tools/datagen.py saas   --scale $(SCALE) --out data/saas
data-ticks:  ; $(PY) tools/datagen.py ticks  --scale $(SCALE) --out data/ticks
data-events: ; $(PY) tools/datagen.py events --scale $(SCALE) --out data/events

citus:
	for step in up cluster load naive plan migrate verify queries colocation rebalance skew; do \
	  citus/scripts/citus.sh $$step || exit 1; done

tsdb:
	for step in up load keys chunks compress caggs retention vacuum; do \
	  timescaledb/scripts/tsdb.sh $$step || exit 1; done

ch:
	for step in up schema load variants mv parts failover; do \
	  clickhouse/scripts/ch.sh $$step || exit 1; done

compare:
	compare/scripts/compare.sh data
	compare/scripts/compare.sh all

down:
	citus/scripts/citus.sh down || true
	timescaledb/scripts/tsdb.sh down || true
	clickhouse/scripts/ch.sh down || true
	docker compose -f compare/docker-compose.yml --profile pg --profile citus --profile tsdb --profile ch down -v || true
