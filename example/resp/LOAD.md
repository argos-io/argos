# Connection-level defaults (task 7.5) — confirmed

Evidence from `example/resp` Sequential × tcp load tests.
§6.1 connection defaults are **confirmed** (values unchanged from the prior placeholders).

Re-run:

```bash
go test ./example/resp/ -race -count=1 -timeout 300s \
  -run 'Burst|Idle|Lifetime|Inbound' -v
```

## Confirmed defaults

| Option | Value | Rationale (measured) |
|--------|------:|----------------------|
| MaxIdleSessions | 8 | After a 64-wide held burst, wave2 ΔHELLO=56; a follow-up of size 8 reuses fully (ΔHELLO=0). Avoids parking 64 idle sockets. |
| SessionIdleTimeout | 50s | Reclaim verified; aligned under typical ~60s peer/LB idle so the client drops first |
| MaxSessionLifetime | 30m | Marks non-reusable after age; in-flight calls survive; aligned with `MaxInboundConnAge` |
| MaxInboundConns | 1024 | Reject beyond limit (`ConnPhaseAdmit` / `ResourceExhausted`); `Serve` continues; 1024×64KiB ≈ 64MiB conn-buffer budget |
| MaxInboundConnIdle | 50s | Idle inbound closed after timeout; aligned with `SessionIdleTimeout` |
| MaxInboundConnAge | 30m | Stops new accepts on aged conn and drains; `Serve` still accepts fresh conns |

The three pool rows (`MaxIdleSessions`, `SessionIdleTimeout`, `MaxSessionLifetime`) are limits of the **axis' pool**: they are fixed when the axis is constructed (`example/resp.WithPool`, plus `WithHandshakeTimeout`), not in `argos.Options`, which no longer carries them. The connection rows (`MaxInboundConns`, `MaxInboundConnIdle`, `MaxInboundConnAge`) are still `argos.Options` fields.

## 1. MaxIdleSessions — burst → idle → burst

Axis options: `WithPool(64, maxIdle, 0, 0)` — burst=64 held mid-flight (no Sequential intra-wave reuse), idle gap=300ms; the two zeros mean "off" for idle reclaim and lifetime cap (on the axis a zero is literal).

Representative run (`TestBurstIdleBurstMaxIdleSessions`):

| MaxIdleSessions | Wave1 dials/HELLO | Wave2 Δdials / ΔHELLO |
|----------------:|------------------:|----------------------:|
| 0 | 64 / 64 | 64 / 64 |
| 8 | 64 / 64 | **56 / 56** |
| 64 | 64 / 64 | 0 / 0 |

| Follow-up | Result |
|-----------|--------|
| After idle=8 large burst, second held burst of **8** | ΔHELLO = **0** |

**Keep 8:** idle=0 forces a full re-handshake every wave; idle=64 retains an entire concurrency-width idle set against the peer; idle=8 keeps a small cushion (follow-up ≤8 free) while shedding 56 sockets after a wide burst — exactly the trade §6.1 described.

## 2. SessionIdleTimeout reclaim

`TestSessionIdleTimeoutReclaim` with `SessionIdleTimeout=200ms` (pool reclaim tick=1s):

| Step | Δdials | ΔHELLO |
|------|-------:|-------:|
| PING, wait idleTO+2.5s, PING again | 1 | 1 |

Enforcement is live. Default **50s** stays: below common ~60s LB/peer idle so the client reclaims before the peer FINs a pooled session.

## 3. MaxSessionLifetime (non-reusable, in-flight safe)

`TestMaxSessionLifetimeNonReusable` with `MaxSessionLifetime=150ms`:

| Check | Result |
|-------|--------|
| In-flight PING spanning lifetime | succeeds |
| Next Open after Close | Δdials=1, ΔHELLO=1 |

Default **30m** kept, aligned with `MaxInboundConnAge`.

## 4. MaxInboundConns (tcp)

`TestMaxInboundConnsRejectsBeyondLimit` with limit=2:

| Check | Result |
|-------|--------|
| 3rd inbound while 2 held | `ConnPhaseAdmit` + `ResourceExhausted` (count≥1) |
| `Serve` after reject | still running |
| Free one slot, new HELLO | succeeds |

Default **1024** kept (reject path real; budget 1024×`ConnReadBufferSize`64KiB ≈ 64MiB).

## 5. MaxInboundConnIdle (tcp)

`TestMaxInboundConnIdleCloses` with idle=120ms: idle inbound read returns `EOF` after timeout.

Default **50s** kept, matched to `SessionIdleTimeout`.

## 6. MaxInboundConnAge (tcp)

`TestMaxInboundConnAgeDrains` with age=200ms:

| Check | Result |
|-------|--------|
| Aged inbound | read `EOF` (accept loop cancelled / drain) |
| Fresh dial after drain | HELLO succeeds (`Serve` continues) |

Default **30m** kept, matched to `MaxSessionLifetime`.
