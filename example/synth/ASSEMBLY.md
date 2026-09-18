# Assembly evidence — `example/synth` (tasks 7.3 / 7.4 / 7.2b)

## Zero core API change

Implementing this synthetic Framing gate required **no** changes to public
interfaces in:

- `transport/` (root interfaces)
- `protocol/` (root interfaces)
- `client/`
- `server/`

Only files under `example/synth/` were added or modified. ConnState reaches
server handlers via connection-ctx `ContextWith` or a Sequential same-goroutine
handoff — not a new core API.

If a future change to those core public surfaces is needed to keep this package
green, that is a design defect — record it here.

## Commits

| Commit | Summary |
|--------|---------|
| `f759768` | `feat(example/synth): greeting-first Sequential framing for assembly gate` (7.3/7.4) |

## What this proves

1. Greeting-first handshake inside `NewServerSession` (`HandshakeTimeout`)
2. Sequential reuse, no metadata channel, custom on-wire method field
3. Connection-level state via `ConnState` + `FromContext`
4. Exclusive mode: `Reusable() == false`; one bidi call carries many exchanges
