# Running the conformance harness

## One-time setup: start the VM oracle

```sh
docker run -d --rm \
  --name esm-oracle-vm \
  -p 18430:8428 \
  victoriametrics/victoria-metrics:v1.144.0
```

## Run scenarios

```sh
# All scenarios:
cargo run --release --bin conformance-harness -- --vm-tag v1.144.0 run

# Single scenario:
cargo run --release --bin conformance-harness -- --vm-tag v1.144.0 run smoke

# Dry-run (no docker / esm-single spawn):
cargo run --release --bin conformance-harness -- dry-run smoke

# List + check YAML well-formedness:
cargo run --release --bin conformance-harness -- list
cargo run --release --bin conformance-harness -- check
```

## Run the live-VM integration test suite

```sh
export VM_URL=http://127.0.0.1:18430
cargo test --release --workspace
```

When `VM_URL` is unset the live tests
(`vm_writeback`, `vm_differential_codec`) print a skip notice and pass
without exercising VM. Everything else (including
`vm_fixtures` against a committed snapshot of VM output) runs
unconditionally.

## Teardown

```sh
docker stop esm-oracle-vm
```

The container was started with `--rm` so stop also removes it. The
on-disk `victoria-metrics-data` volume is recreated fresh each time.
