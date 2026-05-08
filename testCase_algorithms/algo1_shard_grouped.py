"""
algo1_shard_grouped.py
======================
Algorithm 1: Shard-Grouped Ordering
-------------------------------------

EXECUTION MODEL ASSUMPTION (stated explicitly):
  This algorithm operates under Model B — reordered block semantics.
  The proposer has freedom to choose any transaction ordering before
  committing the block. The chosen ordering then becomes the canonical
  execution order. Readers see the state produced by whatever precedes
  them in the COMMITTED block, not the original mempool order.

  Under Model B, this algorithm is a valid communication-reduction
  heuristic. Under Model A (serial block semantics where the original
  order defines canonical execution), this algorithm is NOT
  semantics-preserving because it may reorder conflicting same-shard
  transactions, changing what readers see.

  If your blockchain uses Model A, use Algorithm 2 instead.
  If your blockchain uses Model B (proposer-chosen ordering), this
  algorithm is both valid and highly effective.

WHAT IT DOES:
  Sort all transactions by home shard. All Shard-0 transactions run
  first as a contiguous group, then Shard-1, then Shard-2, then
  Shard-3. Order within each shard group is preserved from the
  original block ordering.

WHY IT REDUCES MESSAGES UNDER MODEL B:
  When all Shard-1 transactions run before Shard-0 transactions in
  the committed block, Shard-1 reads the initial (pre-block) state of
  resources owned by Shard-0. No message needed — initial state is
  available locally. The grouping converts cross-shard reads into
  free local reads wherever no same-shard write precedes them.

LIMITATION:
  Does not reason about individual transaction dependencies.
  May change execution semantics (unsafe under Model A).
  Can degrade near-zero workloads (see NFT anomaly in run_all.py).

COMPLEXITY: O(n log n) — one sort pass.
"""

import random
from collections import defaultdict
from shared import (
    Tx, generate_block, cross_shard_messages, communication_rounds,
    dependency_edges, print_metrics, NUM_SHARDS, SEED, BLOCK_SIZE
)


def shard_grouped(txs):
    """
    Group all transactions by home shard.
    Within each shard group, preserve original relative order.
    Operates under Model B (reordered block defines canonical semantics).
    """
    groups = defaultdict(list)
    for tx in txs:
        groups[tx.home_shard()].append(tx)

    result = []
    for shard_id in range(NUM_SHARDS):
        result.extend(groups[shard_id])
    return result


def test_toy_example():
    print("=" * 62)
    print("ALGO 1: SHARD-GROUPED — Toy example (Ray's a,b,c,d)")
    print("Model B: reordered block defines canonical semantics")
    print("=" * 62)

    a = Tx(0, reads=[1], writes=[2], load_type="toy")
    b = Tx(1, reads=[2], writes=[3], load_type="toy")
    c = Tx(2, reads=[3], writes=[4], load_type="toy")
    d = Tx(3, reads=[4], writes=[5], load_type="toy")
    txs = [a, b, c, d]

    print("\nOriginal order: a b c d")
    print(f"  Shards: a@S{a.home_shard()} b@S{b.home_shard()} "
          f"c@S{c.home_shard()} d@S{d.home_shard()}")

    naive_m = cross_shard_messages(txs)
    naive_r = communication_rounds(txs)
    print(f"  Naive: {naive_m} messages, {naive_r} rounds")

    grouped = shard_grouped(txs)
    names   = {id(a): "a", id(b): "b", id(c): "c", id(d): "d"}
    order_str = " ".join(names[id(t)] for t in grouped)
    gm = cross_shard_messages(grouped)
    gr = communication_rounds(grouped)
    print(f"\nGrouped order: {order_str}")
    print(f"  Grouped: {gm} messages, {gr} rounds")
    print(f"\nNote: under Model B this is valid — the committed block")
    print(f"  order is c d a b and readers see that canonical state.")
    print(f"  Under Model A this would change execution results.")


def test_workloads():
    print("\n" + "=" * 62)
    print("ALGO 1: SHARD-GROUPED — All workloads (Model B baseline)")
    print("=" * 62)

    for lt in ["P2P", "DEX_AVG", "DEX_BURSTY", "NFT", "MIXED"]:
        rng     = random.Random(SEED)
        txs     = generate_block(BLOCK_SIZE, lt, rng)
        naive_m = cross_shard_messages(txs)
        naive_r = communication_rounds(txs)
        grouped = shard_grouped(list(txs))

        print(f"\n  Workload: {lt}")
        print(f"  Naive baseline: msgs={naive_m}, rounds={naive_r}")
        print_metrics("Shard-grouped (Model B)", grouped, naive_m, naive_r)

        from collections import Counter
        counts    = Counter(tx.home_shard() for tx in txs)
        load_str  = "  ".join(f"S{s}:{counts[s]}" for s in range(NUM_SHARDS))
        print(f"  Shard load: {load_str}")


if __name__ == "__main__":
    test_toy_example()
    test_workloads()
