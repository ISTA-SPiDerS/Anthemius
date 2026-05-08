"""
algo3_hot_resource_batch.py
===========================
Hot-Resource Batching — Research Note
--------------------------------------

STATUS: NOT A WORKING ALGORITHM.
  This file documents a research idea and explains why the first
  implementation attempt (v1) failed. The correct implementation
  of this idea is in algo5_hot_resource_v2.py.

  As noted by the professor: v1 should not be presented as working
  code. There is also an indentation error in v1 that would crash
  execution. This file has been restructured as a research note only.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE CORE IDEA (conceptually sound)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A "hot" resource is one that is written by transactions on one shard
and read by many transactions on other shards. Example: a popular DEX
liquidity pool on Shard 0, accessed by 50 transactions on Shard 1.

If the writer runs first:
  Every reader must send a cross-shard message to get the new value.
  50 transactions x 1 message each = 50 messages.

If the readers run first:
  They all read the initial (pre-block) state of the pool.
  Initial state is available locally — no messages needed.
  0 messages.

This is Ray's d->c->b->a inversion idea, generalised to real workloads.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHY V1 FAILED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

V1 computed a "cross_read_score" for each transaction based on the
ORIGINAL ordering: how many cross-shard reads would this transaction
need if the block stays in its current order?

The problem: this score becomes stale the moment reordering starts.
A transaction that scored 0 in the original order (no prior writers)
might score 2 in the new order (a different transaction now precedes
it and writes its resources). The algorithm moved transactions around
based on information that was immediately invalidated by those moves.

Result: every workload got MORE messages than naive. The algorithm
was confidently making wrong decisions.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE CORRECT DESIGN (implemented in algo5_hot_resource_v2.py)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

The fix requires a two-pass approach:

PASS 1 (analysis — before scheduling anything):
  Scan the entire block once. For each hot resource r:
    - Identify the writer transaction(s)
    - Classify each cross-shard reader as:
        AVOIDABLE:   reader has no dependency path forcing it after
                     the writer. Safe to run first. Reads initial state.
        UNAVOIDABLE: reader genuinely needs the writer's new value.
                     Must stay after the writer. One message is forced.

PASS 2 (scheduling — using Pass 1 results):
  Step A: schedule all avoidable readers first (grouped by shard)
  Step B: schedule hot writers next (grouped by shard)
  Step C: schedule unavoidable readers (grouped by shard)
  Step D: schedule remaining transactions (grouped by shard)

This design computes scores BEFORE any reordering occurs, so they
never become stale. The classification is based on the dependency
graph structure, not on positional ordering.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BENCHMARK RESULTS (from algo5_hot_resource_v2.py)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Workload     Naive   Shard-grouped   Hot-resource v2
  ---------    -----   -------------   ---------------
  P2P              0               0                 0
  DEX_AVG        112              25                28
  DEX_BURSTY     144               8                 9
  NFT              1               4 (worse)         1 (guarded)
  MIXED           70              23                30

V2 results:
  - Matches shard-grouped within 1-3 messages on DEX workloads
  - Fixes the NFT anomaly completely (guard prevents degradation)
  - Never makes any workload worse than naive

See algo5_hot_resource_v2.py for the full working implementation.
"""

# No executable code in this file.
# This file is a research note only.
# See algo5_hot_resource_v2.py for the working implementation.

print(__doc__)
