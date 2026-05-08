"""
algo5_hot_resource_v2.py
========================
Algorithm 5: Two-Pass Hot-Resource Communication Heuristic
-----------------------------------------------------------

EXECUTION MODEL ASSUMPTION (stated explicitly):
  This algorithm operates under Model B — reordered block semantics.
  The proposer freely chooses any transaction ordering before
  committing the block. The committed ordering then defines canonical
  execution semantics. Readers see the state produced by whatever
  precedes them in the COMMITTED block.

  This algorithm is NOT semantics-preserving under Model A (serial
  block semantics). Scheduling a reader before a writer changes what
  the reader sees — the reader now sees initial state rather than the
  writer's output. Under Model A this is illegal. Under Model B it is
  the intended behaviour: the proposer deliberately places readers
  first so they read initial state and require no cross-shard messages.

  If your blockchain uses Model A, use Algorithm 2.
  If your blockchain uses Model B, this algorithm is a valid and
  effective communication heuristic.

REMOVED CLAIM (not proven, removed per professor's correction):
  The previous version claimed:
    "lower bound = number of unavoidable cross-shard dependencies"
  This is not proven and is likely false in general. Multiple writers,
  multi-resource transactions, and group interactions can create new
  dependencies after scheduling that the simple count does not capture.
  The actual lower bound requires reasoning about the entire conflict
  graph structure. This claim has been removed.

  What we can say empirically: on the tested workloads, this algorithm
  achieves message counts close to shard-grouped (within 1-5 messages)
  and never makes any workload worse than naive. That is an empirical
  observation, not a theorem.

WHAT IT DOES:
  PASS 1 — Analysis (scan the entire block before scheduling anything):
    (a) Find hot resources: written on one shard, read cross-shard.
    (b) For each cross-shard reader of a hot resource, classify as:
          AVOIDABLE:   no conflict path forces this reader after the
                       writer. Scheduling it first causes it to read
                       initial state — zero messages under Model B.
          UNAVOIDABLE: a conflict path forces this reader after the
                       writer. It must see the writer's output.
                       One message is required regardless of ordering.
    (c) NFT guard: if naive message count is already at or below the
        unavoidable count, skip reordering — it cannot help.

  PASS 2 — Scheduling using Pass 1 classification:
    Group A: avoidable readers   (read initial state — 0 msgs each)
    Group B: hot writers
    Group C: unavoidable readers (need writer's value — 1 msg each)
    Group D: remaining transactions
    Within each group: shard-affinity ordering (same shard contiguous).

RELATIONSHIP TO ALGORITHM 2:
  Algorithm 2 is the semantics-preserving formal core (Model A).
  Algorithm 5 is a communication heuristic (Model B).
  They are not comparable — they solve different problems under
  different execution models.
"""

import random
from collections import defaultdict
from shared import (
    Tx, generate_block, cross_shard_messages, communication_rounds,
    dependency_edges, print_metrics, shard_of,
    NUM_SHARDS, SEED, BLOCK_SIZE
)


# ═══════════════════════════════════════════════════════════════════════════
# PASS 1: ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

def build_conflict_graph(txs):
    """
    Full conflict graph: write->read, read->write, write->write
    on every resource, same-shard and cross-shard.
    Used for reachability checks in Pass 1 analysis.
    conflict_deps[j] = set of indices that must precede j under Model A.
    Under Model B we use this only to check reachability, not to
    enforce ordering.
    """
    conflict_deps = defaultdict(set)
    last_writer   = {}
    last_readers  = defaultdict(list)

    for j, tx in enumerate(txs):
        for r in tx.reads:
            if r in last_writer:
                conflict_deps[j].add(last_writer[r])
        for r in tx.writes:
            if r in last_writer:
                conflict_deps[j].add(last_writer[r])
            for prev in last_readers[r]:
                conflict_deps[j].add(prev)
            last_writer[r]  = j
            last_readers[r] = []
        for r in tx.reads:
            last_readers[r].append(j)

    return conflict_deps


def analyse_block(txs):
    """
    Pass 1: classify cross-shard readers as avoidable or unavoidable.
    Returns counts for empirical reporting (not formal lower bounds).

    avoidable:   readers that CAN be placed before their writer
                 under Model B (no conflict path forces them after)
    unavoidable: readers that MUST come after their writer even under
                 Model B (a conflict path through another resource
                 forces the ordering)
    hot_writers: writers of hot resources
    """
    n = len(txs)

    writers = defaultdict(list)
    readers = defaultdict(list)
    for i, tx in enumerate(txs):
        for r in tx.writes: writers[r].append(i)
        for r in tx.reads:  readers[r].append(i)

    hot_resources = set()
    for r, wlist in writers.items():
        r_shard = shard_of(r)
        for rdr in readers[r]:
            if txs[rdr].home_shard() != r_shard:
                hot_resources.add(r)
                break

    conflict_deps = build_conflict_graph(txs)

    def can_reach(source, target):
        visited, stack = set(), [source]
        while stack:
            node = stack.pop()
            if node == target:  return True
            if node in visited: continue
            visited.add(node)
            for j in range(n):
                if node in conflict_deps.get(j, set()):
                    stack.append(j)
        return False

    avoidable   = set()
    unavoidable = set()
    hot_writers = set()

    for r in hot_resources:
        if not writers[r]: continue
        w_idx   = writers[r][-1]
        r_shard = shard_of(r)
        hot_writers.add(w_idx)

        for rdr in readers[r]:
            if txs[rdr].home_shard() == r_shard:
                continue
            if can_reach(w_idx, rdr):
                unavoidable.add(rdr)
            else:
                avoidable.add(rdr)

    return avoidable, unavoidable, hot_writers


def should_skip(txs, unavoidable_count):
    """
    NFT guard: if naive messages <= unavoidable count,
    reordering is unlikely to help. Return original order.
    This is a heuristic guard, not a proven optimality condition.
    """
    return cross_shard_messages(txs) <= unavoidable_count


# ═══════════════════════════════════════════════════════════════════════════
# PASS 2: SCHEDULING
# ═══════════════════════════════════════════════════════════════════════════

def shard_affinity_sort(indices, txs):
    """Sort indices by home shard, preserving relative order within shard."""
    groups = defaultdict(list)
    for i in indices:
        groups[txs[i].home_shard()].append(i)
    result = []
    for s in range(NUM_SHARDS):
        result.extend(groups[s])
    return result


def hot_resource_v2(txs):
    """
    Two-pass communication heuristic under Model B.
    See module docstring for full specification and assumptions.
    """
    n = len(txs)

    avoidable, unavoidable, hot_writers = analyse_block(txs)
    unavoidable_count = len(unavoidable)

    if should_skip(txs, unavoidable_count):
        return list(txs)

    classified = avoidable | unavoidable | hot_writers
    remaining  = set(range(n)) - classified

    group_a = shard_affinity_sort(avoidable,   txs)
    group_b = shard_affinity_sort(hot_writers, txs)
    group_c = shard_affinity_sort(unavoidable, txs)
    group_d = shard_affinity_sort(remaining,   txs)

    return [txs[i] for i in group_a + group_b + group_c + group_d]


# ═══════════════════════════════════════════════════════════════════════════
# TEST CASES
# ═══════════════════════════════════════════════════════════════════════════

def test_model_b_reordering():
    """
    Shows Model B semantics explicitly.
    tx0 writes res 0 (S0). tx1 reads res 0 (S1).
    Under Model A: swapping is illegal — tx1 sees wrong value.
    Under Model B: proposer places tx1 first in committed block.
                   tx1 reads initial state. 0 messages. Valid.
    """
    print("=" * 62)
    print("ALGO 5 TEST 1: Model B reordering")
    print("Proposer places reader before writer in committed block.")
    print("Reader sees initial state. Valid under Model B only.")
    print("=" * 62)

    tx0 = Tx(0, reads=[4],    writes=[0], load_type="test")  # S0 writer
    tx1 = Tx(1, reads=[5, 0], writes=[5], load_type="test")  # S1 reader
    tx2 = Tx(2, reads=[9, 0], writes=[9], load_type="test")  # S1 reader
    tx3 = Tx(3, reads=[1],    writes=[1], load_type="test")  # S1 unrelated
    txs = [tx0, tx1, tx2, tx3]

    print("\nTransactions:")
    for tx in txs: print(f"  {tx}")

    naive_m = cross_shard_messages(txs)
    av, unav, hw = analyse_block(txs)
    print(f"\nNaive: {naive_m} msgs")
    print(f"Pass 1: avoidable={sorted(av)} "
          f"unavoidable={sorted(unav)} writers={sorted(hw)}")
    print(f"Unavoidable count (empirical): {len(unav)}")

    ordered   = hot_resource_v2(list(txs))
    order_str = " ".join(f"tx{t.tx_id}" for t in ordered)
    om        = cross_shard_messages(ordered)
    print(f"\nV2 order (committed block): {order_str}")
    print(f"V2 messages: {om}")
    print(f"\nModel B note: in the committed block, readers precede writer.")
    print(f"  Readers see initial state of res 0 — no message needed.")
    print(f"  Under Model A this would be semantically incorrect.")


def test_nft_guard():
    """NFT workload: guard prevents degradation."""
    print("\n" + "=" * 62)
    print("ALGO 5 TEST 2: NFT guard")
    print("=" * 62)

    rng     = random.Random(SEED)
    txs     = generate_block(BLOCK_SIZE, "NFT", rng)
    naive_m = cross_shard_messages(txs)
    _, unav, _ = analyse_block(txs)
    fired   = should_skip(txs, len(unav))

    print(f"Naive messages    : {naive_m}")
    print(f"Unavoidable count : {len(unav)}")
    print(f"Guard fires       : {fired}")

    ordered  = hot_resource_v2(list(txs))
    result_m = cross_shard_messages(ordered)
    print(f"V2 messages       : {result_m}")
    print("✓ Guard worked" if result_m <= naive_m else "✗ Guard failed")


def test_workloads():
    """
    Empirical benchmark. Results are observations, not proofs.
    Lower bound column shows unavoidable count as a reference point
    only — it is not proven to be the true lower bound.
    """
    print("\n" + "=" * 62)
    print("ALGO 5: COMMUNICATION HEURISTIC (Model B) — All workloads")
    print("Empirical results only. No formal lower bound claimed.")
    print("=" * 62)

    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    from algo1_shard_grouped import shard_grouped

    for lt in ["P2P", "DEX_AVG", "DEX_BURSTY", "NFT", "MIXED"]:
        rng     = random.Random(SEED)
        txs     = generate_block(BLOCK_SIZE, lt, rng)
        naive_m = cross_shard_messages(txs)
        naive_r = communication_rounds(txs)
        _, unav, _ = analyse_block(txs)

        print(f"\n  Workload: {lt}  "
              f"(naive: {naive_m} msgs, {naive_r} rounds, "
              f"unavoidable count: {len(unav)} [not proven lower bound])")
        print_metrics("Naive",              list(txs),
                      naive_m, naive_r)
        print_metrics("Shard-grouped (B)",  shard_grouped(list(txs)),
                      naive_m, naive_r)
        print_metrics("Hot-resource v2 (B)",hot_resource_v2(list(txs)),
                      naive_m, naive_r)


if __name__ == "__main__":
    test_model_b_reordering()
    test_nft_guard()
    test_workloads()
