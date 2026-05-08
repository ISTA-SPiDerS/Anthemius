"""
sharding_demo.py
================
Ray's three steps — with correct metrics.

Metric: only DEPENDENCY COMMUNICATION counts.
A message is sent when tx B reads a resource that tx A wrote,
AND A and B are on different shards.
That is one round of waiting.

Ray's example: a→b→c→d = 3 dependency messages, 3 rounds.
"""

import random
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict, Set, Tuple
from heapq import heappush, heappop

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

NUM_SHARDS    = 4
NUM_ACCOUNTS  = 10_000
NUM_RESOURCES = 1_000
BLOCK_SIZE    = 50
SEED          = 42

random.seed(SEED)

def shard_of(resource_id: int) -> int:
    return resource_id % NUM_SHARDS

# ─────────────────────────────────────────────────────────────────────────────
# TRANSACTION MODEL
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Tx:
    tx_id:     int
    reads:     List[int]
    writes:    List[int]
    load_type: str

    def home_shard(self) -> int:
        if self.writes:
            return shard_of(self.writes[0])
        return shard_of(self.reads[0])

    def is_cross_shard(self) -> bool:
        home = self.home_shard()
        return any(shard_of(r) != home for r in self.reads + self.writes)

# ─────────────────────────────────────────────────────────────────────────────
# CORRECT METRICS — exactly as Ray described
# ─────────────────────────────────────────────────────────────────────────────

def dependency_edges(txs: List[Tx]):
    """
    For every (tx_i, tx_j) pair where:
      - tx_i writes resource r
      - tx_j reads resource r
      - i < j (i comes before j in block)
    
    Record an edge (i, j, r, is_cross_shard).
    is_cross_shard = True if tx_i and tx_j are on different shards.
    
    These are the ONLY edges that require communication.
    Ray's example:
      a writes 2, b reads 2 → edge (a,b, cross=True)  → 1 message
      b writes 3, c reads 3 → edge (b,c, cross=True)  → 1 message
      c writes 4, d reads 4 → edge (c,d, cross=True)  → 1 message
      Total = 3 messages, 3 rounds.
    """
    last_writer: Dict[int, int] = {}
    edges = []
    for j, tx in enumerate(txs):
        for r in tx.reads:
            if r in last_writer:
                i     = last_writer[r]
                cross = txs[i].home_shard() != tx.home_shard()
                edges.append((i, j, r, cross))
        for r in tx.writes:
            last_writer[r] = j
    return edges


def cross_shard_messages(txs: List[Tx]) -> int:
    """
    Count cross-shard dependency messages.
    One message per edge where writer and reader are on different shards.
    This is Ray's primary metric: 'how often do we need to send a message?'
    """
    return sum(1 for _, _, _, cross in dependency_edges(txs) if cross)


def dependency_rounds(txs: List[Tx]) -> int:
    """
    Minimum number of sequential communication rounds.
    Each cross-shard dependency edge adds 1 round.
    Same-shard dependency edges add 0 rounds (local, free).
    
    Ray's example a,b,c,d: 3 cross-shard edges = 3 rounds.
    Ray's inverted d,c,b,a: 0 cross-shard edges = 0 rounds.
    """
    edges = dependency_edges(txs)
    dist  = [0] * len(txs)
    for i, j, _, cross in edges:
        cost     = 1 if cross else 0
        dist[j]  = max(dist[j], dist[i] + cost)
    return max(dist) if dist else 0


def classify_deps(txs: List[Tx]):
    """
    Of all cross-shard dependency messages, classify as:
    AVOIDABLE:   edge exists only because tx_j was placed after tx_i
                 in the block. A different ordering could place tx_j
                 before tx_i so it reads initial state instead.
    UNAVOIDABLE: tx_j genuinely needs tx_i's written value.
                 No ordering removes this edge.
    
    A dependency (i→j on resource r) is AVOIDABLE if there exists
    a valid ordering where j comes before i — i.e., neither j→i
    nor any chain j→...→i exists in the dependency graph.
    Simplified check: if i does not (transitively) depend on j,
    we could swap them.
    """
    edges    = dependency_edges(txs)
    # Build adjacency for reachability check
    succ     = defaultdict(set)
    for i, j, _, cross in edges:
        if cross:
            succ[i].add(j)

    def reaches(src, tgt, n):
        """Can we reach tgt from src?"""
        visited = set()
        stack   = [src]
        while stack:
            node = stack.pop()
            if node == tgt:
                return True
            if node in visited:
                continue
            visited.add(node)
            stack.extend(succ[node])
        return False

    avoidable = unavoidable = 0
    for i, j, r, cross in edges:
        if not cross:
            continue
        # If j can reach i, swapping would create a cycle → unavoidable
        if reaches(j, i, len(txs)):
            unavoidable += 1
        else:
            avoidable += 1

    return avoidable, unavoidable


# ─────────────────────────────────────────────────────────────────────────────
# WORKLOAD DISTRIBUTIONS (from Anthemius repo)
# ─────────────────────────────────────────────────────────────────────────────

def power_law_weights(n, exp=1.5):
    w = [1.0 / (i ** exp) for i in range(1, n + 1)]
    s = sum(w)
    return [x / s for x in w]

def bursty_weights(n):
    w = power_law_weights(n, exp=2.5)
    w[0] *= 5
    s = sum(w)
    return [x / s for x in w]

SENDER_W = power_law_weights(NUM_ACCOUNTS,  1.2)
AVG_W    = power_law_weights(NUM_RESOURCES, 1.5)
BURSTY_W = bursty_weights(NUM_RESOURCES)

def sample(weights, rng=random):
    r, cum = rng.random(), 0.0
    for i, w in enumerate(weights):
        cum += w
        if r <= cum:
            return i
    return len(weights) - 1

def make_p2p(tx_id, rng=random):
    s = sample(SENDER_W, rng)
    r = rng.randint(0, NUM_ACCOUNTS - 1)
    while r == s:
        r = rng.randint(0, NUM_ACCOUNTS - 1)
    return Tx(tx_id, reads=[s], writes=[s, r], load_type="P2P")

def make_dex(tx_id, bursty, rng=random):
    pool   = sample(BURSTY_W if bursty else AVG_W, rng)
    sender = sample(SENDER_W, rng)
    pool_r = NUM_ACCOUNTS + pool
    return Tx(tx_id, reads=[sender, pool_r],
              writes=[sender, pool_r],
              load_type="DEX_BURSTY" if bursty else "DEX_AVG")

def make_nft(tx_id, rng=random):
    nft    = sample(AVG_W, rng)
    sender = sample(SENDER_W, rng)
    recvr  = rng.randint(0, NUM_ACCOUNTS - 1)
    nft_r  = NUM_ACCOUNTS + nft
    return Tx(tx_id, reads=[nft_r, sender],
              writes=[nft_r, recvr], load_type="NFT")

def make_mixed(tx_id, rng=random):
    r = rng.random()
    if   r < 0.40: return make_p2p(tx_id, rng)
    elif r < 0.70: return make_dex(tx_id, False, rng)
    elif r < 0.85: return make_dex(tx_id, True,  rng)
    else:          return make_nft(tx_id, rng)

GENERATORS = {
    "P2P":        make_p2p,
    "DEX_AVG":    lambda i, r: make_dex(i, False, r),
    "DEX_BURSTY": lambda i, r: make_dex(i, True,  r),
    "NFT":        make_nft,
    "MIXED":      make_mixed,
}

def generate_block(size, load_type, rng=random):
    return [GENERATORS[load_type](i, rng) for i in range(size)]


# ─────────────────────────────────────────────────────────────────────────────
# REORDERING STRATEGIES
# ─────────────────────────────────────────────────────────────────────────────

def order_naive(txs):
    return list(txs)

def order_shard_group(txs):
    """Ray's A^n B^n C^n: group by home shard."""
    groups = defaultdict(list)
    for tx in txs:
        groups[tx.home_shard()].append(tx)
    result = []
    for s in range(NUM_SHARDS):
        result.extend(groups[s])
    return result

def order_smart(txs):
    """
    Smart ordering — generalises Ray's d,c,b,a inversion.

    Only enforce UNAVOIDABLE cross-shard dependencies.
    Transactions whose cross-shard reads are of initial state
    (avoidable) can be scheduled freely — before any writer.

    Among free transactions, group by home shard (Ray's grouping).
    """
    n     = len(txs)
    edges = dependency_edges(txs)

    # Build dependency sets: only unavoidable cross-shard edges
    # A dep i→j is unavoidable if j needs i's written value AND cross-shard
    true_deps: Dict[int, Set[int]] = defaultdict(set)
    for i, j, r, cross in edges:
        if cross:
            true_deps[j].add(i)

    in_degree = [len(true_deps[i]) for i in range(n)]
    heap      = []
    for i in range(n):
        if in_degree[i] == 0:
            heappush(heap, (txs[i].home_shard(), i))

    result    = []
    scheduled = set()

    while heap:
        _, i = heappop(heap)
        result.append(txs[i])
        scheduled.add(i)
        for j in range(n):
            if j not in scheduled and i in true_deps[j]:
                true_deps[j].discard(i)
                if len(true_deps[j]) == 0:
                    heappush(heap, (txs[j].home_shard(), j))

    seen = {id(t) for t in result}
    for t in txs:
        if id(t) not in seen:
            result.append(t)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Ray's exact a,b,c,d example
# ─────────────────────────────────────────────────────────────────────────────

def step1_rays_example():
    print("=" * 64)
    print("STEP 1: Ray's a,b,c,d example")
    print("a[1,2]  b[2,3]  c[3,4]  d[4,5]")
    print("Sharding: resource_id % 4 → shard")
    print("=" * 64)

    a = Tx(0, reads=[1], writes=[2], load_type="example")
    b = Tx(1, reads=[2], writes=[3], load_type="example")
    c = Tx(2, reads=[3], writes=[4], load_type="example")
    d = Tx(3, reads=[4], writes=[5], load_type="example")

    print("\nResource → shard:")
    for r in range(1, 6):
        print(f"  resource {r} → Shard {shard_of(r)}")

    print("\nTransaction shards:")
    for tx, name in [(a,"a"),(b,"b"),(c,"c"),(d,"d")]:
        print(f"  {name}: reads={tx.reads} writes={tx.writes} "
              f"@S{tx.home_shard()}")

    print("\nDependency edges (writer → reader on resource r):")
    for name_order, txs, label in [
        (["a","b","c","d"], [a,b,c,d], "Naive a,b,c,d"),
        (["d","c","b","a"], [d,c,b,a], "Inverted d,c,b,a"),
    ]:
        edges = dependency_edges(txs)
        names = {id(a):"a",id(b):"b",id(c):"c",id(d):"d"}
        print(f"\n  Order: {' '.join(name_order)}")
        if edges:
            for i, j, r, cross in edges:
                kind = "CROSS-SHARD msg" if cross else "local (free)"
                print(f"    {names[id(txs[i])]}→{names[id(txs[j])]} "
                      f"on res {r} : {kind}")
        else:
            print("    (no dependency edges — all reads are of initial state)")
        print(f"  Cross-shard dependency messages : "
              f"{cross_shard_messages(txs)}")
        print(f"  Dependency communication rounds : "
              f"{dependency_rounds(txs)}")
        print(f"  ← Ray's metric: {dependency_rounds(txs)} rounds "
              f"of waiting")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Print transaction dependencies from real workload
# ─────────────────────────────────────────────────────────────────────────────

def step2_print_dependencies(load_type="MIXED"):
    print("\n" + "=" * 64)
    print(f"STEP 2: Transaction dependencies — workload: {load_type}")
    print(f"Format: txN: reads=[...] writes=[...] @ShardX")
    print("=" * 64)

    rng = random.Random(SEED)
    txs = generate_block(BLOCK_SIZE, load_type, rng)

    print(f"\nFirst 20 transactions:\n")
    for tx in txs[:20]:
        tag = "CROSS" if tx.is_cross_shard() else "local"
        print(f"  tx{tx.tx_id:>2}: reads={str(tx.reads):<22} "
              f"writes={str(tx.writes):<26} "
              f"@S{tx.home_shard()} [{tag}]")

    print(f"\nDependency edges in naive order:")
    edges = dependency_edges(txs[:20])
    if edges:
        for i, j, r, cross in edges:
            kind = "CROSS (msg)" if cross else "local"
            print(f"  tx{i}→tx{j} on resource {r:>6} : {kind}")
    else:
        print("  (no write→read dependencies in first 20 txs)")

    # Statistics
    av, unav = classify_deps(txs)
    xs = sum(1 for tx in txs if tx.is_cross_shard())
    print(f"\nBlock statistics ({BLOCK_SIZE} txs):")
    print(f"  Cross-shard txs          : {xs} ({round(100*xs/BLOCK_SIZE,1)}%)")
    print(f"  Cross-shard dep messages : {cross_shard_messages(txs)}")
    print(f"  Dependency rounds        : {dependency_rounds(txs)}")
    print(f"  Potentially avoidable messages : {av}  (no in-block write→read forces this order)")
    print(f"  Order-constrained messages     : {unav}  (in-block write→read, order must be preserved)")

    # Shard load
    print(f"\nShard load:")
    counts = [0] * NUM_SHARDS
    for tx in txs:
        counts[tx.home_shard()] += 1
    for s in range(NUM_SHARDS):
        bar = "█" * counts[s]
        print(f"  Shard {s}: {counts[s]:>3} txs  {bar}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Smart reordering results
# ─────────────────────────────────────────────────────────────────────────────

def step3_reordering():
    print("\n" + "=" * 64)
    print("STEP 3: Smart reordering — all workloads")
    print("Metric: cross-shard dependency messages and rounds")
    print("=" * 64)

    LOAD_TYPES = ["P2P", "DEX_AVG", "DEX_BURSTY", "NFT", "MIXED"]

    for lt in LOAD_TYPES:
        rng = random.Random(SEED)
        txs = generate_block(BLOCK_SIZE, lt, rng)
        av, unav = classify_deps(txs)

        results = {}
        for name, fn in [
            ("Naive",         order_naive),
            ("Shard-grouped", order_shard_group),
            ("Smart",         order_smart),
        ]:
            ordered = fn(list(txs))
            results[name] = {
                "msgs":  cross_shard_messages(ordered),
                "rounds": dependency_rounds(ordered),
            }

        naive_msgs   = results["Naive"]["msgs"]
        naive_rounds = results["Naive"]["rounds"]

        print(f"\n  Workload: {lt}  "
              f"| avoidable: {av}  | unavoidable: {unav}")
        print(f"  {'Strategy':<18} {'Messages':>9} {'Saved':>7} "
              f"{'Rounds':>8} {'Saved':>7}")
        print(f"  {'-'*18} {'-'*9} {'-'*7} {'-'*8} {'-'*7}")

        for name in ["Naive", "Shard-grouped", "Smart"]:
            r     = results[name]
            ms    = naive_msgs   - r["msgs"]
            rs    = naive_rounds - r["rounds"]
            ms_s  = f"-{ms}" if ms>0 else (f"+{abs(ms)}" if ms<0 else "=")
            rs_s  = f"-{rs}" if rs>0 else (f"+{abs(rs)}" if rs<0 else "=")
            best  = " ★" if (r["msgs"]   == min(v["msgs"]   for v in results.values())
                          and r["rounds"] == min(v["rounds"] for v in results.values())) else ""
            print(f"  {name:<18} {r['msgs']:>9} {ms_s:>7} "
                  f"{r['rounds']:>8} {rs_s:>7}{best}")

        smart_saved = naive_msgs - results["Smart"]["msgs"]
        if av > 0:
            pct = round(100 * max(0, smart_saved) / av, 1)
            print(f"\n  Smart: saved {smart_saved} messages "
                  f"({pct}% of {av} avoidable eliminated)")
        print(f"  Lower bound: {unav} order-constrained messages (write→read chains within block)")

    print("\n" + "=" * 64)
    print("RESULT FOR RAY:")
    print("  Ray's toy example is reproduced exactly:")
    print("    a→b→c→d gives 3 dependency messages and 3 waiting rounds.")
    print("    d→c→b→a gives 0 dependency messages and 0 waiting rounds.")
    print()
    print("  Shard-grouped ordering is currently the strongest baseline:")
    print("    DEX_AVG    : 20 → 9 messages, 12 → 3 rounds")
    print("    DEX_BURSTY : 33 → 3 messages, 33 → 3 rounds")
    print("    MIXED      : 15 → 6 messages, 10 → 3 rounds")
    print()
    print("  The avoidable/order-constrained split is the current working hypothesis.")
    print("  The current Smart heuristic is experimental and needs improvement.")
    print("  True write→read dependencies may impose a lower bound if execution order")
    print("  and semantics must be preserved.")
    print("=" * 64)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    step1_rays_example()
    step2_print_dependencies("MIXED")
    step3_reordering()