.PHONY: build test bench demo up down logs clean

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build:
	cargo build --release

build-debug:
	cargo build

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

test:
	cargo test --workspace

test-verbose:
	cargo test --workspace -- --nocapture

# ---------------------------------------------------------------------------
# Docker stack
# ---------------------------------------------------------------------------

up:
	docker compose -f deploy/docker-compose.yml up -d --build

down:
	docker compose -f deploy/docker-compose.yml down -v

logs:
	docker compose -f deploy/docker-compose.yml logs -f

# ---------------------------------------------------------------------------
# Demo (the "make demo" end-to-end walkthrough)
# ---------------------------------------------------------------------------

demo: up demo-wait demo-run

demo-wait:
	@echo "Waiting for services to become healthy..."
	@for i in $$(seq 1 30); do \
	    if curl -sf http://127.0.0.1:6400/health >/dev/null 2>&1 && \
	       curl -sf http://127.0.0.1:6402/health >/dev/null 2>&1; then \
	        echo "All services healthy."; break; \
	    fi; \
	    echo "  waiting... ($$i/30)"; sleep 5; \
	done

demo-run:
	@echo ""
	@echo "=== Lattice End-to-End Demo ==="
	@echo ""
	@echo "-- Step 1: Create tenant --"
	$(eval TENANT := $(shell curl -sf -X POST http://127.0.0.1:6402/tenants \
	    -H "Content-Type: application/json" \
	    -d '{"name":"demo-tenant"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['tenant_id'])"))
	@echo "  tenant_id = $(TENANT)"

	@echo ""
	@echo "-- Step 2: Create root timeline --"
	$(eval ROOT_TL := $(shell curl -sf -X POST http://127.0.0.1:6400/timelines \
	    -H "Content-Type: application/json" \
	    -d '{"tenant_id":"$(TENANT)","name":"main"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['timeline_id'])"))
	@echo "  timeline_id = $(ROOT_TL)"

	@echo ""
	@echo "-- Step 3: Branch (O(1) copy-on-write) --"
	@curl -sf -X POST http://127.0.0.1:6402/branches \
	    -H "Content-Type: application/json" \
	    -d "{\"tenant_id\":\"$(TENANT)\",\"parent_timeline_id\":\"$(ROOT_TL)\",\"branch_lsn\":1,\"name\":\"feature-branch\"}" \
	    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  branch created in {d[\"elapsed_us\"]} μs → {d[\"timeline_id\"]}')"

	@echo ""
	@echo "-- Step 4: Autoscaler decisions (open http://localhost:3000) --"
	@curl -sf http://127.0.0.1:6402/autoscaler/decisions | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  {len(d)} decisions recorded')"

	@echo ""
	@echo "Demo complete. Services:"
	@echo "  Pageserver:    http://127.0.0.1:6400"
	@echo "  Control Plane: http://127.0.0.1:6402"
	@echo "  Grafana:       http://localhost:3000  (admin/lattice)"
	@echo "  MinIO:         http://localhost:9001  (lattice/lattice123)"
	@echo "  Prometheus:    http://localhost:9090"

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

bench: bench-branch bench-storage

bench-branch:
	@chmod +x bench/scenarios/branch_latency.sh
	@./bench/scenarios/branch_latency.sh

bench-storage:
	@chmod +x bench/scenarios/storage_amplification.sh
	@./bench/scenarios/storage_amplification.sh

bench-autoscaler:
	@chmod +x bench/scenarios/autoscaler_demo.sh
	@./bench/scenarios/autoscaler_demo.sh

# ---------------------------------------------------------------------------
# C extension
# ---------------------------------------------------------------------------

build-smgr:
	cd lattice_smgr && make

install-smgr:
	cd lattice_smgr && make install

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------

clean:
	cargo clean
	docker compose -f deploy/docker-compose.yml down -v --remove-orphans
