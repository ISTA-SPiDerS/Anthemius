"""
ordering_algorithms.py
======================
Implements Ray's four ordering algorithms plus baselines.
Primary metric: cross-shard dependency messages (write->read pairs across shards).
Secondary metric: dependency communication rounds (critical path depth).

Ray's framing: block-proposer-level reordering under fixed shard assignment.
Goal: reduce how many times execution on one shard must wait for a value
      produced by another shard.
"""

import random
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict, Set, Tuple
from heapq import heappush, heappop

# ── Configuration ──────────────────────────────────────────────────────────
NUM_SHARDS    = 4
NUM_ACCOUNTS  = 10_000
NUM_RESOURCES = 1_000
BLOCK_SIZE    = 200
SEED          = 42
random.seed(SEED)

def shard_of(r): return r % NUM_SHARDS

# ── Transaction model ──────────────────────────────────────────────────────
@dataclass
class Tx:
    tx_id:     int
    reads:     List[int]
    writes:    List[int]
    load_type: str

    def home_shard(self):
        return shard_of(self.writes[0]) if self.writes else shard_of(self.reads[0])

    def all_resources(self):
        return set(self.reads) | set(self.writes)

# ── Workload distributions ─────────────────────────────────────────────────
def power_law(n, exp=1.5):
    w = [1/(i**exp) for i in range(1,n+1)]
    s = sum(w); return [x/s for x in w]

def bursty(n):
    w = power_law(n, 2.5); w[0] *= 5; s=sum(w); return [x/s for x in w]

SENDER_W = power_law(NUM_ACCOUNTS, 1.2)
AVG_W    = power_law(NUM_RESOURCES, 1.5)
BURSTY_W = bursty(NUM_RESOURCES)

def sample(weights, rng=random):
    r,c = rng.random(),0.0
    for i,w in enumerate(weights):
        c+=w
        if r<=c: return i
    return len(weights)-1

def make_p2p(i,rng=random):
    s=sample(SENDER_W,rng); r=rng.randint(0,NUM_ACCOUNTS-1)
    while r==s: r=rng.randint(0,NUM_ACCOUNTS-1)
    return Tx(i,[s],[s,r],"P2P")

def make_dex(i,bursty_flag,rng=random):
    pool=sample(BURSTY_W if bursty_flag else AVG_W,rng)
    sender=sample(SENDER_W,rng); pr=NUM_ACCOUNTS+pool
    return Tx(i,[sender,pr],[sender,pr],"DEX_BURSTY" if bursty_flag else "DEX_AVG")

def make_nft(i,rng=random):
    nft=sample(AVG_W,rng); s=sample(SENDER_W,rng); r=rng.randint(0,NUM_ACCOUNTS-1)
    nr=NUM_ACCOUNTS+nft; return Tx(i,[nr,s],[nr,r],"NFT")

def make_mixed(i,rng=random):
    r=rng.random()
    if r<0.4:   return make_p2p(i,rng)
    elif r<0.7: return make_dex(i,False,rng)
    elif r<0.85:return make_dex(i,True,rng)
    else:       return make_nft(i,rng)

GENS = {"P2P":make_p2p,"DEX_AVG":lambda i,r:make_dex(i,False,r),
        "DEX_BURSTY":lambda i,r:make_dex(i,True,r),"NFT":make_nft,"MIXED":make_mixed}

def gen_block(size,lt,rng=random):
    return [GENS[lt](i,rng) for i in range(size)]

# ── Core metrics (Ray's definition) ───────────────────────────────────────
def dep_edges(txs):
    """Write->read pairs. Edge (i,j,r,cross): tx[i] writes r, tx[j] reads r, i<j."""
    last = {}; edges = []
    for j,tx in enumerate(txs):
        for r in tx.reads:
            if r in last:
                i=last[r]; cross=(txs[i].home_shard()!=tx.home_shard())
                edges.append((i,j,r,cross))
        for r in tx.writes: last[r]=j
    return edges

def cross_msgs(txs):
    return sum(1 for _,_,_,c in dep_edges(txs) if c)

def comm_rounds(txs):
    edges=dep_edges(txs); dist=[0]*len(txs)
    for i,j,_,c in edges:
        cost=1 if c else 0
        dist[j]=max(dist[j],dist[i]+cost)
    return max(dist) if dist else 0

# ── Algorithm 0: Naive baseline ────────────────────────────────────────────
def order_naive(txs): return list(txs)

# ── Algorithm 1: Shard-grouped ────────────────────────────────────────────
def order_shard_grouped(txs):
    """Group all same-shard txs together: A^n B^n C^n D^n.
    Converts avoidable dependencies into initial-state reads."""
    g=defaultdict(list)
    for tx in txs: g[tx.home_shard()].append(tx)
    return [tx for s in range(NUM_SHARDS) for tx in g[s]]

# ── Algorithm 2: Dependency-aware shard grouping ──────────────────────────
def order_dep_aware_shard(txs):
    """Group by shard BUT preserve unavoidable write->read ordering.

    Unavoidable dependency: tx[j] reads resource r, tx[i] wrote r earlier,
    AND they are on different shards (a genuine cross-shard wait).
    We must keep i before j.

    Strategy: topological sort within a shard-affinity priority.
    Among topologically-ready txs, prefer the one whose home shard
    has the most remaining txs (greedy shard consolidation).
    """
    n=len(txs)
    # Build true deps: only unavoidable cross-shard write->read
    true_deps=defaultdict(set)
    last={}
    for j,tx in enumerate(txs):
        for r in tx.reads:
            if r in last:
                i=last[r]
                if txs[i].home_shard()!=tx.home_shard():
                    true_deps[j].add(i)
        for r in tx.writes: last[r]=j

    in_deg=[len(true_deps[i]) for i in range(n)]
    # Count remaining txs per shard for greedy priority
    shard_count=defaultdict(int)
    for tx in txs: shard_count[tx.home_shard()]+=1

    # heap: (-shard_remaining, tx_index) — prefer shards with more work
    heap=[]
    for i in range(n):
        if in_deg[i]==0:
            heappush(heap,(-shard_count[txs[i].home_shard()],i))

    result=[]; scheduled=set()
    while heap:
        _,i=heappop(heap)
        result.append(txs[i]); scheduled.add(i)
        shard_count[txs[i].home_shard()]-=1
        for j in range(n):
            if j not in scheduled and i in true_deps[j]:
                true_deps[j].discard(i)
                if len(true_deps[j])==0:
                    heappush(heap,(-shard_count[txs[j].home_shard()],j))

    seen={id(t) for t in result}
    for t in txs:
        if id(t) not in seen: result.append(t)
    return result

# ── Algorithm 3: Hot-resource batching ────────────────────────────────────
def order_hot_resource_batch(txs):
    """Detect resources with many cross-shard dependents.
    Place consumers (readers) BEFORE writers when semantics allow.
    This converts avoidable dependencies into initial-state reads.

    Hot resource = written by tx on shard A, read by txs on other shards.
    For those readers that have no prior in-block dependency on the writer,
    schedule them first — they can safely read initial state.
    """
    n=len(txs)

    # Find which resources are written, and by whom
    writers=defaultdict(list)  # resource -> [tx indices that write it]
    for i,tx in enumerate(txs):
        for r in tx.writes: writers[r].append(i)

    # For each tx, compute cross-shard reader score:
    # how many of its reads come from resources written by cross-shard txs?
    # Lower score = fewer dangerous reads = schedule earlier
    cross_read_score=[0]*n
    for j,tx in enumerate(txs):
        for r in tx.reads:
            for w_idx in writers[r]:
                if w_idx<j and txs[w_idx].home_shard()!=tx.home_shard():
                    cross_read_score[j]+=1

    # Build true deps (same as dep_aware)
    true_deps=defaultdict(set)
    last={}
    for j,tx in enumerate(txs):
        for r in tx.reads:
            if r in last:
                i=last[r]
                if txs[i].home_shard()!=tx.home_shard():
                    true_deps[j].add(i)
        for r in tx.writes: last[r]=j

    in_deg=[len(true_deps[i]) for i in range(n)]
    heap=[]
    for i in range(n):
        if in_deg[i]==0:
            # Sort by: (cross_read_score asc, home_shard asc)
            heappush(heap,(cross_read_score[i], txs[i].home_shard(), i))

    result=[]; scheduled=set()
    while heap:
        _,_,i=heappop(heap)
        result.append(txs[i]); scheduled.add(i)
        for j in range(n):
            if j not in scheduled and i in true_deps[j]:
                true_deps[j].discard(i)
                if len(true_deps[j])==0:
                    heappush(heap,(cross_read_score[j],txs[j].home_shard(),j))

    seen={id(t) for t in result}
    for t in txs:
        if id(t) not in seen: result.append(t)
    return result

# ── Algorithm 4: Greedy communication-minimizing ──────────────────────────
def order_greedy_comm_min(txs):
    """At each step, pick the ready transaction that adds the fewest
    new cross-shard dependency messages and minimises dependency depth.

    Ray's specification: 'choose the transaction that adds the fewest
    new cross-shard dependency messages and shortest dependency depth.'

    Implementation:
    - 'ready' = all true deps satisfied
    - score = (new_cross_msgs_added, dep_depth_so_far, home_shard)
    - pick lowest score
    """
    n=len(txs)

    # Pre-compute which resources each tx reads from cross-shard writers
    # We need this to estimate cost of scheduling tx[j] at position p
    # given what has already been scheduled.

    # Build write registry per resource
    write_order=defaultdict(list)
    for i,tx in enumerate(txs):
        for r in tx.writes: write_order[r].append(i)

    # True deps
    true_deps_orig=defaultdict(set)
    last={}
    for j,tx in enumerate(txs):
        for r in tx.reads:
            if r in last:
                i=last[r]
                if txs[i].home_shard()!=tx.home_shard():
                    true_deps_orig[j].add(i)
        for r in tx.writes: last[r]=j

    true_deps={j:set(v) for j,v in true_deps_orig.items()}
    in_deg=[len(true_deps.get(i,set())) for i in range(n)]

    # Track current state
    scheduled=set()   # set of tx indices already placed
    last_writer={}    # resource -> last scheduled tx index that wrote it
    dist=[0]*n        # current dep depth for each tx

    heap=[]
    for i in range(n):
        if in_deg[i]==0:
            heappush(heap,(0, txs[i].home_shard(), i))

    result=[]
    while heap:
        _,_,i=heappop(heap)
        result.append(txs[i]); scheduled.add(i)

        # Update last_writer for writes of tx[i]
        for r in txs[i].writes:
            last_writer[r]=i

        # Unlock dependent txs and compute their new cost estimate
        for j in range(n):
            if j not in scheduled and i in true_deps.get(j,set()):
                true_deps[j].discard(i)
                if len(true_deps.get(j,set()))==0:
                    # Compute cost of scheduling j now
                    new_msgs=0
                    max_pred_depth=0
                    for r in txs[j].reads:
                        if r in last_writer:
                            w=last_writer[r]
                            if txs[w].home_shard()!=txs[j].home_shard():
                                new_msgs+=1
                                max_pred_depth=max(max_pred_depth,dist[w])
                    dist[j]=max_pred_depth+(1 if new_msgs>0 else 0)
                    heappush(heap,(new_msgs, txs[j].home_shard(), j))

    seen={id(t) for t in result}
    for t in txs:
        if id(t) not in seen: result.append(t)
    return result

# ── Benchmark ─────────────────────────────────────────────────────────────
STRATEGIES = [
    ("Naive",                  order_naive),
    ("Shard-grouped",          order_shard_grouped),
    ("Dep-aware shard",        order_dep_aware_shard),
    ("Hot-resource batch",     order_hot_resource_batch),
    ("Greedy comm-min",        order_greedy_comm_min),
]

LOAD_TYPES = ["P2P","DEX_AVG","DEX_BURSTY","NFT","MIXED"]

def run():
    print("="*72)
    print("CROSS-SHARD DEPENDENCY COMMUNICATION — ALGORITHM COMPARISON")
    print(f"Block size: {BLOCK_SIZE}  |  Shards: {NUM_SHARDS}")
    print("Primary metric: cross-shard dependency messages (write->read across shards)")
    print("Secondary metric: communication rounds (critical path depth)")
    print("="*72)

    all_results = {}

    for lt in LOAD_TYPES:
        rng=random.Random(SEED)
        txs=gen_block(BLOCK_SIZE,lt,rng)
        naive_msgs=cross_msgs(order_naive(txs))
        naive_rounds=comm_rounds(order_naive(txs))

        print(f"\n  Workload: {lt}  (naive baseline: {naive_msgs} msgs, {naive_rounds} rounds)")
        print(f"  {'Algorithm':<26} {'Msgs':>6} {'Saved':>7} {'Rounds':>8} {'Saved':>7} {'Best?':>6}")
        print(f"  {'-'*26} {'-'*6} {'-'*7} {'-'*8} {'-'*7} {'-'*6}")

        results=[]
        for name,fn in STRATEGIES:
            ordered=fn(list(txs))
            m=cross_msgs(ordered); r=comm_rounds(ordered)
            results.append((name,m,r))

        best_m=min(r[1] for r in results)
        best_r=min(r[2] for r in results)

        for name,m,r in results:
            ms=naive_msgs-m; rs=naive_rounds-r
            ms_s=f"-{ms}" if ms>0 else (f"+{abs(ms)}" if ms<0 else "=")
            rs_s=f"-{rs}" if rs>0 else (f"+{abs(rs)}" if rs<0 else "=")
            best="★" if m==best_m and r==best_r else ("m★" if m==best_m else ("r★" if r==best_r else ""))
            print(f"  {name:<26} {m:>6} {ms_s:>7} {r:>8} {rs_s:>7} {best:>6}")

        all_results[lt]=results

    # Summary table
    print("\n" + "="*72)
    print("SUMMARY: messages saved vs naive baseline")
    print(f"  {'Algorithm':<26}", end="")
    for lt in LOAD_TYPES: print(f" {lt:>12}", end="")
    print()
    print(f"  {'-'*26}", end="")
    for _ in LOAD_TYPES: print(f" {'-'*12}", end="")
    print()

    for idx,(name,_) in enumerate(STRATEGIES):
        print(f"  {name:<26}", end="")
        for lt in LOAD_TYPES:
            results=all_results[lt]
            naive_m=results[0][1]  # naive is first
            m=results[idx][1]
            saved=naive_m-m
            tag=f"-{saved}" if saved>0 else (f"+{abs(saved)}" if saved<0 else "=")
            print(f" {tag:>12}", end="")
        print()

    print("\n" + "="*72)
    print("Ray's framing: proposer-time reordering, fixed shard assignment.")
    print("Goal: reduce write->read cross-shard waits before execution.")
    print("Best algorithm = lowest msgs + lowest rounds.")
    print("="*72)

if __name__=="__main__":
    run()
