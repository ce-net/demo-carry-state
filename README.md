# A cell that takes its data with it

A notes cell runs in process A, and a client writes to it 20 times a second. You type `move --to b`.
A sends B a snapshot while it keeps serving, then freezes writes for a few milliseconds, sends B
the last few writes and a handoff it has signed, and B takes over under a higher epoch. From then on
A answers stale writes with `moved, epoch 2`. The client always calls the same name, `notes`. After
each move the demo counts writes sent, writes acknowledged and writes present in the store, and the
three counts match.

```
move 1: a -> b under epoch 2: writes frozen 13.68 ms (snapshot at #22, pre-copied 0 ops, frozen tail 0 ops)
  hash chain before 748bb0e2d708 #22  after 748bb0e2d708 #22
  old holder now answers: "moved, epoch 2"
  writes sent 43  acknowledged 43  present 43  (lost 0, duplicated 0)  longest client wait 3.6 ms  OK
```

## Run it in under 5 minutes

You need Rust 1.88 or newer. Every dependency is public. The open capability token
[`ce-net-proto`](https://github.com/ce-net/ce-net-proto) comes in as a pinned git dependency, and the rest
comes from crates.io.

```sh
git clone https://github.com/ce-net/demo-demo-carry-state
cd demo-demo-carry-state
cargo run -- up            # processes A and B, the name service, a client at 20 writes/s, the page
# open http://127.0.0.1:7400/ and press "Move to B", or in a second terminal:
cargo run -- move --to b
cargo run -- move --to a
```

`up` prints the pid and address of each process. Each `move` prints the freeze time, the hash-chain head
on both sides and the reply the old holder now gives, then audits the store against every write the
client sent.

To run the whole proof yourself:

```sh
cargo test --release                                   # unit tests plus 6 moves under load, end to end
cargo run --release -- prove --moves 10 --out run.json # the recorded run below
cargo run --release -- prove --moves 100 --clients 8 --rate 200 --settle-ms 100   # what CI runs
```

`prove` exits 1 if a single write is lost or duplicated, if the hash chain differs between the two
sides, or if the old holder accepts a write after the move.

### Through the local dev sim

The clients can find the name service the way they would on ce-net: the demo lists it on
[ce-net-local-dev-sim](https://github.com/ce-net/ce-net-local-dev-sim) (`app_publish`), the clients look it up
with `apps_search`, and one `name.resolve` call also goes through the sim's `mcp_use`. You need Python 3.8
or newer.

```sh
git clone https://github.com/ce-net/ce-net-local-dev-sim /tmp/local-dev-sim
python3 /tmp/local-dev-sim/cenet_mock.py 7878 &
cargo run -- up --sim http://127.0.0.1:7878/mcp
```

## Recorded proof

Ten moves with one client writing 20 times a second, recorded on a 16-core arm64 Linux machine
([`proof/moves-10.txt`](proof/moves-10.txt), [`proof/moves-10.json`](proof/moves-10.json)). "Frozen" is how long
A held the write lock, from the moment it froze to the moment B had taken over and A answered "moved". The
counts are taken after each move, once the in-flight writes have finished.

| # | move | epoch | frozen ms | frozen tail | sent | acked | present | chain head before (A) | chain head after (B) | A then answers |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | A to B | 2 | 13.68 | 0 | 43 | 43 | 43 | `748bb0e2d7086c9f` | `748bb0e2d7086c9f` | moved, epoch 2 |
| 2 | B to A | 3 | 25.81 | 0 | 65 | 65 | 65 | `3d07667ceabab3fe` | `3d07667ceabab3fe` | moved, epoch 3 |
| 3 | A to B | 4 | 12.94 | 0 | 87 | 87 | 87 | `e4019e1cd0d26829` | `e4019e1cd0d26829` | moved, epoch 4 |
| 4 | B to A | 5 | 9.36 | 0 | 110 | 110 | 110 | `63ef787a2adee040` | `63ef787a2adee040` | moved, epoch 5 |
| 5 | A to B | 6 | 55.92 | 0 | 133 | 133 | 133 | `e287e91ba19741c5` | `e287e91ba19741c5` | moved, epoch 6 |
| 6 | B to A | 7 | 35.78 | 1 | 156 | 156 | 156 | `d4e6b175a1f09445` | `d4e6b175a1f09445` | moved, epoch 7 |
| 7 | A to B | 8 | 11.36 | 0 | 179 | 179 | 179 | `cfa92a1c054c6b9e` | `cfa92a1c054c6b9e` | moved, epoch 8 |
| 8 | B to A | 9 | 37.86 | 0 | 201 | 201 | 201 | `b1dc7a8722889339` | `b1dc7a8722889339` | moved, epoch 9 |
| 9 | A to B | 10 | 5.16 | 0 | 223 | 223 | 223 | `f8a573f0630ba1fc` | `f8a573f0630ba1fc` | moved, epoch 10 |
| 10 | B to A | 11 | 2.24 | 0 | 245 | 245 | 245 | `4e2e72b5a31aee93` | `4e2e72b5a31aee93` | moved, epoch 11 |

After the last move the chain head on the holder was `ef1cc27c54a22c32…`. Recomputing the chain from the
first write over every note in the store gives the same head.

A second run did 100 moves with 8 clients at 200 writes/s each, and the clients found the name service
through the local dev sim ([`proof/moves-100.txt`](proof/moves-100.txt), [`proof/moves-100.json`](proof/moves-100.json)).
It ended with 30010 writes sent, 30010 acknowledged and 30010 present, none lost and none duplicated.
Pre-copy sent up to 519 ops per move while A kept serving, and the tail sent while frozen was never longer
than 8 ops.

About the freeze times: the fastest moves in these runs froze writes for 1.5 to 2.3 ms. Both runs were
recorded while other jobs kept the machine at a load average of about 56 on 16 cores. That is why the medians
are 13.7 ms (10 moves) and 7.3 ms (100 moves) and the worst freezes reach 56 ms and 191 ms. Almost all of that
is time spent waiting for a CPU, because the tail was empty or a few ops. Correctness does not depend on timing:
every run, idle or loaded, ended with the three counts equal.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs the tests and then the 100-move run against a
fresh local dev sim on every push. It fails on any lost or duplicated write.

## How a move works

1. **Snapshot, still serving.** A copies its state (all notes, plus the highest write number applied for
   each client) together with the index and chain head at that point. It sends the copy to B with its
   capability chain. B unpacks it and computes its digest before anything freezes.
2. **Pre-copy, still serving.** A sends whatever was written in the meantime, in up to five rounds, until
   fewer than nine ops remain.
3. **Freeze.** A takes the write lock. Incoming writes wait. A signs a handoff that says: name `notes`,
   epoch E+1, from A's key to B's key at B's address, snapshot digest D, ending at index I with chain head H.
   It sends B the handoff and the last ops.
4. **B checks and takes over.** B verifies A's signature. It verifies A's
   [ce-net-proto](https://github.com/ce-net/ce-net-proto) capability chain from the operator's root, which must
   grant `cell.hold`. It checks that the snapshot matches D and that replaying the ops lands exactly on I and H.
   Then B countersigns and presents both signatures, plus its own `cell.hold` chain, to the name service.
5. **Name moves only forward.** The name service accepts a handoff only for exactly the next epoch, and only
   from the key that holds the name now. Replays, stale epochs and handoffs from a former holder are refused.
6. **Unfreeze as "moved".** Every write that was waiting at A, and every later one, gets
   `moved, epoch E+1`. The client asks the name service where `notes` lives now and sends the same write again
   to B. Each client numbers its writes, so a write sent twice is applied once.

If the handoff fails, A asks the name service before it serves again. If the name service cannot answer, A
stops serving. The point is never to have two holders.

## Crates

| Crate | Cell | Job |
|---|---|---|
| [`crates/cell-migrate`](crates/cell-migrate) | `cell.migrate` | Generic over any `Cell` that applies ops deterministically: the journal with its SHA-256 hash chain, snapshot, pre-copy, tail, the signed `Handoff` and its countersignature, and `check_handoff` (signature, `cell.hold` chain, digest, replay to the signed head) |
| [`crates/name-resolve`](crates/name-resolve) | `name.resolve` | Maps a name to its holder, address and epoch. Accepts a root-signed genesis, then only verified handoffs to the next epoch |
| [`crates/notes-store`](crates/notes-store) | `notes.store` | The sample stateful cell: append-only notes, a per-client write number for dedupe, and the lost/duplicated audit |
| `src/` | the demo | Processes A and B (`carry-state cell`), the name service served over MCP JSON-RPC (`name.resolve`, `name.handoff`), the clients, `up`, `move`, `prove` and the page |

A and B are separate OS processes with their own Ed25519 keys. A new set of keys is generated for each run.
The name service and the clients run in the `up` process. Everything binds to 127.0.0.1.

## License

Apache-2.0. See [LICENSE](LICENSE).
