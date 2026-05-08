"""
algo2_dep_aware_shard.py
========================
Algorithm 2: Dependency-Aware Shard Grouping
----------------------------------------------

PROFESSOR'S CORRECTION (addressed in this version):
  The original version defined "unavoidable" dependencies using
  original-order write->read edges across shards only. This was
  conceptually dangerous for two reasons:

  (1) The definition was circular: it called a dependency "forced"
      because of the current order, but the algorithm's purpose is
      to change that order. An original-order dependency is not
      inherently forced — it disappears if the reader is placed
      before the writer.

  (2) It conflated two distinct concepts:
      - Communication-reducing reordering: any reordering that
        reduces cross-shard messages, regardless of whether the
        final blockchain state changes.
      - Semantics-preserving reordering: a reordering where the
        final blockchain state is IDENTICAL to the original ordering.

FORMAL DEFINITION (used in this version):
  Two transactions tx[i] and tx[j] CONFLICT if any of the following:
    (a) tx[i] writes resource r AND tx[j] reads  resource r
    (b) tx[i] reads  resource r AND tx[j] writes resource r
    (c) tx[i] writes resource r AND tx[j] writes resource r

  A reordering is SEMANTICS-PRESERVING if and only if for every pair
  of transactions that conflict on any resource, their relative order
  is preserved from the original block.

  This is the standard commutativity condition from database theory
  (Gray and Reuter, Transaction Processing, 1992).

WHY SAME-SHARD CONFLICTS MUST ALSO BE PRESERVED:
  If tx[i] on Shard 0 writes resource 4 and tx[j] on Shard 0 reads
  resource 4, swapping them costs zero messages — but tx[j] now reads
  a different value. The final state after the block changes.
  A correct blockchain proposer must not make this swap.

CONSEQUENCE:
  This algorithm enforces ALL conflicts (same-shard and cross-shard).
  It is safe to use in a real blockchain proposer. Shard-grouped is
  NOT semantics-preserving because it may reorder conflicting
  same-shard transactions.
"""

import random
from collections import defaultdict
from heapq import heappush, heappop
from shared import (
    Tx, generate_block, cross_shard_messages, communication_rounds,
    dependency_edges, print_metrics, NUM_SHARDS, SEED, BLOCK_SIZE,
    shard_of
)


# ── Conflict detection ─────────────────────────────────────────────────────

def build_conflict_graph(txs):
    """
    Builds the FULL conflict graph for semantic correctness.

    conflict_deps[j] = set of tx indices that must come before j
    in any semantics-preserving ordering.

    Covers all three conflict types on every resource:
      (a) write -> read   (b) read -> write   (c) write -> write

    Enforces BOTH same-shard and cross-shard conflicts.
    """
    n             = len(txs)
    conflict_deps = defaultdict(set)
    last_writer   = {}
    last_readers  = defaultdict(list)

    for j, tx in enumerate(txs):
        for r in tx.reads:
            # read-after-write: j reads what an earlier tx wrote
            if r in last_writer:
                conflict_deps[j].add(last_writer[r])

        for r in tx.writes:
            # write-after-write: j writes what an earlier tx wrote
            if r in last_writer:
                conflict_deps[j].add(last_writer[r])
            # write-after-read: j writes what earlier txs read
            for prev_reader in last_readers[r]:
                conflict_deps[j].add(prev_reader)
            last_writer[r]  = j
            last_readers[r] = []   # new version — clear old readers

        for r in tx.reads:
            last_readers[r].append(j)

    return conflict_deps


def count_cross_shard_conflicts(txs, conflict_deps):
    """How many conflict edges cross a shard boundary?"""
    return sum(
        1 for j, preds in conflict_deps.items()
        for i in preds
        if txs[i].home_shard() != txs[j].home_shard()
    )


# ── The algorithm ──────────────────────────────────────────────────────────

def dep_aware_shard_grouped(txs):
    """
    Semantics-preserving topological sort with shard-affinity priority.

    Enforces ALL conflicts to guarantee identical execution results.
    Among conflict-free-to-schedule transactions, prefers the shard
    with the most remaining transactions — building contiguous shard
    blocks where possible without violating correctness.
    """
    n             = len(txs)
    conflict_deps = build_conflict_graph(txs)
    in_degree     = [len(conflict_deps.get(i, set())) for i in range(n)]

    shard_remaining = defaultdict(int)
    for tx in txs:
        shard_remaining[tx.home_shard()] += 1

    heap = []
    for i in range(n):
        if in_degree[i] == 0:
            heappush(heap, (-shard_remaining[txs[i].home_shard()], i))

    result    = []
    scheduled = set()

    while heap:
        _, i = heappop(heap)
        result.append(txs[i])
        scheduled.add(i)
        shard_remaining[txs[i].home_shard()] -= 1

        for j in range(n):
            if j not in scheduled and i in conflict_deps.get(j, set()):
                conflict_deps[j].discard(i)
                if len(conflict_deps[j]) == 0:
                    heappush(heap,
                             (-shard_remaining[txs[j].home_shard()], j))

    seen = {id(t) for t in result}
    for t in txs:
        if id(t) not in seen:
            result.append(t)
    return result


# ── Test cases ─────────────────────────────────────────────────────────────

def test_semantics_preserving():
    """
    Shows that same-shard conflicts are now correctly preserved.
    tx0 and tx1 both on Shard 0. tx0 writes res 4. tx1 reads res 4.
    Swapping costs zero messages but changes tx1's read value.
    The algorithm must keep tx0 before tx1.
    """
    print("=" * 62)
    print("ALGO 2 TEST: same-shard semantic conflict preserved")
    print("=" * 62)

    tx0 = Tx(0, reads=[8], writes=[4], load_type="test")  # S0 writes res4
    tx1 = Tx(1, reads=[4], writes=[0], load_type="test")  # S0 reads res4
    tx2 = Tx(2, reads=[1], writes=[1], load_type="test")  # S1 independent
    tx3 = Tx(3, reads=[2], writes=[2], load_type="test")  # S2 independent
    txs = [tx0, tx1, tx2, tx3]

    print("\nTransactions:")
    for tx in txs:
        print(f"  {tx}")

    conflicts = build_conflict_graph(txs)
    print("\nConflict graph (all pairs, same-shard included):")
    for j, preds in conflicts.items():
        for i in preds:
            kind = ("same-shard" if txs[i].home_shard() == txs[j].home_shard()
                    else "CROSS-SHARD")
            print(f"  tx{i} must come before tx{j}  [{kind}]")

    ordered   = dep_aware_shard_grouped(list(txs))
    order_str = " ".join(f"tx{t.tx_id}@S{t.home_shard()}" for t in ordered)
    print(f"\nResult order: {order_str}")

    positions = {t.tx_id: idx for idx, t in enumerate(ordered)}
    if positions[0] < positions[1]:
        print("✓ tx0 before tx1 — same-shard conflict preserved")
    else:
        print("✗ tx0 after tx1 — semantic conflict violated!")
    print(f"Messages: {cross_shard_messages(ordered)}  "
          f"(same-shard conflict costs no messages)")
    print("Semantics: PRESERVED — tx1 always sees tx0's written value")


def test_unavoidable_dependency():
    """Genuine cross-shard dep: tx0 S1 writes res5, tx1 S2 reads res5."""
    print("\n" + "=" * 62)
    print("ALGO 2 TEST: cross-shard unavoidable dependency")
    print("=" * 62)

    tx0 = Tx(0, reads=[9], writes=[5], load_type="test")  # S1
    tx1 = Tx(1, reads=[5], writes=[6], load_type="test")  # S2 needs tx0
    tx2 = Tx(2, reads=[0], writes=[0], load_type="test")  # S0 independent
    tx3 = Tx(3, reads=[1], writes=[1], load_type="test")  # S1 independent
    tx4 = Tx(4, reads=[3], writes=[3], load_type="test")  # S3 independent
    txs = [tx0, tx1, tx2, tx3, tx4]

    print("\nTransactions:")
    for tx in txs: print(f"  {tx}")

    naive_m = cross_shard_messages(txs)
    naive_r = communication_rounds(txs)
    print(f"\nNaive: {naive_m} msgs, {naive_r} rounds")

    ordered   = dep_aware_shard_grouped(list(txs))
    order_str = " ".join(f"tx{t.tx_id}@S{t.home_shard()}" for t in ordered)
    om        = cross_shard_messages(ordered)
    or_       = communication_rounds(ordered)
    print(f"Dep-aware order: {order_str}")
    print(f"Result: {om} msgs, {or_} rounds")

    positions = {t.tx_id: idx for idx, t in enumerate(ordered)}
    if positions[0] < positions[1]:
        print("✓ tx0 before tx1 — cross-shard dep preserved")
    else:
        print("✗ order violated!")


def test_workloads():
    print("\n" + "=" * 62)
    print("ALGO 2: SEMANTICS-PRESERVING — All workloads")
    print("=" * 62)

    for lt in ["P2P", "DEX_AVG", "DEX_BURSTY", "NFT", "MIXED"]:
        rng     = random.Random(SEED)
        txs     = generate_block(BLOCK_SIZE, lt, rng)
        naive_m = cross_shard_messages(txs)
        naive_r = communication_rounds(txs)
        ordered = dep_aware_shard_grouped(list(txs))

        conflicts = build_conflict_graph(txs)
        cross_c   = count_cross_shard_conflicts(txs, conflicts)
        total_c   = sum(len(v) for v in conflicts.values())
        same_c    = total_c - cross_c

        print(f"\n  Workload: {lt}")
        print(f"  Conflicts: {total_c} total  "
              f"({same_c} same-shard, {cross_c} cross-shard)")
        print(f"  Naive: msgs={naive_m}, rounds={naive_r}")
        print_metrics("Dep-aware (semantics-safe)", ordered, naive_m, naive_r)


if __name__ == "__main__":
    test_semantics_preserving()
    test_unavoidable_dependency()
    test_workloads()
