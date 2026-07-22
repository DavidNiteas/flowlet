# Runtime Redesign Completion Audit

## Purpose

This audit states what current repository evidence proves and what it does not.
It prevents a passing unit suite or a valid sidecar from being treated as
authorization to remove compatibility contracts.

## Audit Date and Scope

Audited: 2026-07-22.

Scope:

- Flowlet framework runtime event/process/projection contracts.
- MetaMSTools and MassLib4Search persisted backend compatibility.
- Real liver study workspace regression.
- Reader, transport, and live framework-observation migration baseline.

Out of scope:

- Numerical quality of annotation models.
- Business workflow semantics beyond preserving existing package behavior.
- External client inventory and release approval, which cannot be proved from
  this repository.

## Requirement Evidence

| Requirement | Status | Authoritative evidence |
| --- | --- | --- |
| Flowlet boundary is business-neutral | proved | `05_boundary_audit.md`; no Flowlet imports from business packages; framework-only APIs documented |
| Standard event envelope/store/SSE exists | proved | `RuntimeEvent`, `RuntimeEventJsonlStore`, `RuntimeEventSidecarWriter`, standard stream helpers; Flowlet runtime tests |
| Standard process/declaration/projection exists | proved | `RuntimeProcessSpec`, process runner/executor, `RuntimeProjection`, `runtime/processes.json`, `runtime/projection.json` tests |
| Existing legacy event/read paths remain usable | proved for repository fixtures | Legacy `events.jsonl`, `/events`, and snapshot tests remain present; historical real workspace reader checks in `07_real_workspace_regression.md` |
| MetaMSTools writes current standard layout | proved | Fresh three-file liver run; strict validator; root declaration, manifest, projection, snapshot reader evidence in `07_real_workspace_regression.md` |
| MassLib4Search writes current standard layout | proved | Fresh workspace annotation run; strict validator; root declaration, manifest, projection, snapshot reader evidence in `07_real_workspace_regression.md` |
| CLI/TUI read projection without fabricating business hierarchy | proved for snapshot reader baseline | Package CLI tests for projection + `status.json`; `08_business_projection_adapters.md` |
| Standard HTTP transport is additive | proved | Both backend `/runtime-events` routes, clients, SSE tests; Meta GUI additive proxy in `11_live_presentation_contract.md` |
| Framework-only live observation is available | proved | `RuntimeObservation`, backend `/runtime-observation`, both clients, Meta GUI proxy; real readback in `07_real_workspace_regression.md` |
| Unavailable standard artifacts are distinguishable | proved | Both backend and Meta GUI `409` tests; `11_live_presentation_contract.md` |
| Legacy removal is safe | **not proved** | External consumer census, version policy approval, standard-only live product coverage, and rollout sign-off are pending |

## Test Evidence

The latest focused evidence recorded in the handoff includes:

- Flowlet runtime suite: 40 passed after `RuntimeObservation` readback work.
- Framework plus both backend suite: 74 passed when the framework observation
  loader was introduced.
- GUI/backend/client focused suite: 69 tests passed when the observation APIs
  were added.
- Real current-layout validator: passes for both fresh liver runtimes.

These are focused regression results, not a claim that every test in the
monorepo has been run after every later documentation-only commit.

## Remaining Decision Gates

The redesign implementation baseline is complete through dual-write and
read-only standard presentation. The following are intentionally pending:

1. External client census for both business packages.
2. Package/product-owner approval of `12_endpoint_version_policy.md`.
3. A package-owned decision on whether live interactive TUI/GUI needs a
   standard framework-observation page beyond the existing SSE/projection
   surfaces.
4. Standard-only writer switch design, implementation, and real regression
   after the preceding approvals.
5. Major-version removal review for legacy writers, while historical legacy
   readers remain supported.

## Completion Decision

Do not mark Phase 9 complete and do not remove legacy event writing. The
framework/runtime redesign is ready for incremental consumers, but legacy
cleanup requires the external approval gates above. Continue to treat the
standard sidecar as canonical framework observability and legacy events as a
supported compatibility contract.
