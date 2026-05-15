# AGENTS.md

This file provides guidance to coding agents (e.g. Claude Code, claude.ai/code) when working with code in this repository.

## Repository purpose

Go module `kubeops.dev/taskqueue` — a Kubernetes operator that maintains a **queue of pending tasks** with a configurable maximum concurrent execution. Defines two CRDs under `batch.k8s.appscode.com/v1alpha1`:

- `TaskQueue` — defines a queue with `MaxConcurrent`, target task type, and selection policy.
- `PendingTask` — enqueued tasks; the controller decides which to start next based on the `TaskQueue` capacity.

Used by other AppsCode operators to throttle expensive cluster-wide actions (e.g. rolling restarts) so they don't all execute simultaneously.

The produced binary is `taskqueue`.

## Architecture

- `cmd/taskqueue/` — entry point.
- `pkg/cmds/` — Cobra root + run.
- `apis/batch/v1alpha1/`:
  - `taskqueue_types.go` — `TaskQueue`.
  - `pendingtask_types.go` — `PendingTask`.
  - `groupversion_info.go`, `doc.go`, generated `zz_generated.deepcopy.go`.
  - `install/`, `fuzzer/`.
- `crds/` — generated CRDs (`batch.k8s.appscode.com_taskqueues.yaml`, `batch.k8s.appscode.com_pendingtasks.yaml`).
- `pkg/controller/` — controller-runtime reconcilers for `TaskQueue` and `PendingTask`.
- `pkg/queue/` — queue execution engine: ordering, capacity tracking, dispatch.
- `config/` — kustomize bases.
- `Dockerfile.in` (PROD, distroless), `Dockerfile.dbg` (debian), `Dockerfile.ubi` (Red Hat certified) — three image variants.
- `PROJECT` — Kubebuilder metadata.
- `hack/`, `Makefile` — AppsCode build harness.
- `vendor/` — checked-in deps.

CRD API group is `batch.k8s.appscode.com`.

## Common commands

All Make targets run inside `ghcr.io/appscode/golang-dev` — Docker must be running.

- `make ci` — CI pipeline.
- `make build` / `make all-build` — build host or all-platform binaries.
- `make gen` — regenerate clientset + manifests. Run after changes to `apis/batch/v1alpha1/*_types.go`.
- `make manifests` — regenerate CRDs only.
- `make clientset` — regenerate client code.
- `make fmt`, `make lint`, `make unit-tests` / `make test` — standard.
- `make verify` — `verify-gen verify-modules`; `go mod tidy && go mod vendor` must leave the tree clean.
- `make container` — build PROD, DBG, and UBI images.
- `make push` — push all three; `make docker-manifest` writes multi-arch manifests; `make release` is the full publish flow.
- `make push-to-kind` / `make deploy-to-kind` — load into Kind and Helm-install.
- `make install` / `make uninstall` / `make purge` — Helm install lifecycle.
- `make add-license` / `make check-license` — manage license headers.

Run a single Go test (requires a local Go toolchain):

```
go test ./pkg/queue/... -run TestName -v
```

## Conventions

- Module path is `kubeops.dev/taskqueue` (vanity URL). Imports must use that.
- License: `LICENSE.md`. Sign off commits (`git commit -s`); contributions follow the DCO.
- Vendor directory is checked in — `go mod tidy && go mod vendor` must leave the tree clean (enforced by `verify-modules`).
- The queue ordering and capacity logic lives in `pkg/queue/` — keep dispatching policy there, not in the reconciler. Reconcilers should be thin.
- Do not hand-edit `zz_generated.*.go` or `crds/*.yaml` — change `apis/batch/v1alpha1/*_types.go` and re-run `make gen`.
- API group is `batch.k8s.appscode.com` (deliberately not the upstream `batch`) — don't rename.
- Three Dockerfiles, one binary — keep `Dockerfile.in`, `Dockerfile.dbg`, and `Dockerfile.ubi` in sync.
- This is a **Kubebuilder project** — use `kubebuilder` to scaffold new APIs.
