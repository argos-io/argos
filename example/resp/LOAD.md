# Connection pool load hints (task 7.5)

Measured on `example/resp` Sequential × tcp with PING bursts.
§6.1 ⚠️ defaults were **not** rewritten — peaks below are observational only.

## Config under test

| Option | Value |
|--------|------:|
| MaxSessionsPerEndpoint | 16 |
| MaxIdleSessions | 4 |
| Burst concurrency | 16 |
| Idle gap | 200ms |

## Measured (representative `go test -run TestBurstIdleBurstLoadHints`)

| Phase | Elapsed | Cumulative dials | Cumulative HELLO |
|-------|--------:|-----------------:|-----------------:|
| Wave 1 (burst) | ~6ms | 16 | 16 |
| Idle | 200ms | (unchanged) | (unchanged) |
| Wave 2 (burst) | ~3ms | 25 | 25 |

| Delta wave2 − wave1 | Value |
|---------------------|------:|
| New dials | 9 |
| New HELLOs | 9 |

## Reading

With `MaxIdleSessions=4` and a burst of 16, the pool retains only a small idle
set after wave 1; wave 2 pays handshake cost again (ΔHELLO≈9). This matches the
§6.1 warning that `MaxIdleSessions(8)` paired with `MaxSessionsPerEndpoint(64)`
closes most sockets between bursts and forces `HELLO`/`AUTH` on the next wave.

No recommendation strong enough to clear the ⚠️ markers on
`MaxIdleSessions` / `SessionIdleTimeout` / `MaxSessionLifetime` /
`MaxInboundConns` / `MaxInboundConnIdle` / `MaxInboundConnAge` was produced by
this pragmatic run — leave §6.1 provisional.

Re-run: `go test ./example/resp/ -run TestBurstIdleBurstLoadHints -v`
