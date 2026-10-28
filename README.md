# netio

Shared packet I/O primitives for [Overture VPN](https://github.com/eukhlg/overture-vpn)
and [ipsec-go](https://github.com/eukhlg/ipsec-go).

| Package | Responsibility |
| --- | --- |
| `packetbuf` | Exclusively owned, pooled packet buffers with headroom/tailroom for VPN encapsulation, plus receive slots |
| `udptransport` | Boundary-preserving UDP batching (`recvmmsg`/`sendmmsg` on Linux, per-datagram fallback elsewhere), a bounded FIFO sender with explicit buffer ownership, and a sharded sender that spreads flows over several senders on one socket, each with its own descriptor |
| `flowhash` | Seeded, allocation-free IPv4 flow hash (5-tuple, 3-tuple for fragments and other protocols) and range reduction onto shards |

The packages were extracted from ipsec-go, where they were introduced for the
ESP fast path, so that plain L2TP and ESP share them without plain L2TP
depending on the IPsec module. History for these paths is preserved.

`packetbuf.Headroom`/`Tailroom` are sized for the PPP/L2TP/UDP/ESP stack used by
Overture VPN; they are not a general-purpose choice.

This module depends only on `golang.org/x/net`. It knows nothing about L2TP,
PPP, IKE, or ESP framing.
