# Assembly evidence — `example/resp` (tasks 7.1 / 7.2 / 7.2b)

## Zero core API change

Implementing this RESP2 gate required **no** changes to public interfaces in:

- `transport/` (root interfaces)
- `protocol/` (root interfaces; not concrete subpackages like `grpc` unless wiring)
- `client/`
- `server/`

Only files under `example/resp/` were added or modified for 7.1/7.2.

If a future change to those core public surfaces is needed to keep this package
green, that is a design defect — record it here instead of papering over it in
the composition layer.

## Commits

| Commit | Summary |
|--------|---------|
| `7f931f8` | `feat(example/resp): RESP2 Sequential framing over tcp` (7.1) |
| `30894e4` | `feat(example/resp): SUBSCRIBE server-streaming exclusive session` (7.2) |

## What this proves

1. Sequential reuse with connection-level `HELLO`/`AUTH`
2. Missing metadata channel → `SendHeaders` = `Unimplemented` without a kernel hole
3. `SUBSCRIBE` as server-streaming: `Reusable() == false`, pushes via subsequent `Recv`, connection closed (not pooled) when the call ends
