"""
Flajolet-Martin Algorithm — Approximate Distinct Element Counting
==================================================================

The Flajolet-Martin (FM) algorithm estimates the number of distinct elements
in a data stream using O(log n) memory instead of O(n) for exact counting.

HOW IT WORKS (Mining of Massive Datasets, Ch. 4.4):

1. HASH + TRAILING ZEROS:
   For each incoming element x, compute h(x) (a hash function mapping to
   an integer). Count the number of trailing zeros r(x) in the binary
   representation of h(x).

   The intuition is that:
   - About 50% of hash values end in 0  (r >= 1)
   - About 25% end in 00               (r >= 2)
   - About 12.5% end in 000            (r >= 3)
   - About 1/2^r end in r zeros        (r >= r)

   So if we've seen n distinct elements, we EXPECT to have seen at least
   one element with ~log₂(n) trailing zeros.

2. TRACK MAXIMUM R:
   We maintain R = max r(x) over all elements seen. The estimate of
   distinct elements is 2^R.

3. REDUCING VARIANCE:
   A single hash function gives a very noisy estimate (can be off by
   factors of 2). To improve accuracy, we use MULTIPLE independent hash
   functions, group them, and combine:

   - Use L independent hash functions, divided into groups of size g.
   - Within each group, take the MEDIAN of 2^R values (kills the heavy
     tail: a single huge R would dominate an average of 2^R).
   - Across groups, take the AVERAGE of the medians.
   - Scale by the classic FM constant phi ≈ 0.77351.

   Averaging 2^R first (median-of-averages) looks like the MMDS sketch
   but is still biased high: E[2^R] is many times n because 2^R has an
   extremely heavy right tail. Average-of-medians + phi is the stable
   combination for small and moderate n.

WHY APPROXIMATE?
  Counting exact distinct elements in a stream requires storing every
  unique element seen — O(n) memory. FM uses only O(log n) memory by
  maintaining a single integer R per hash function. With 128 hash functions,
  we use only ~1 KB regardless of stream size.
"""

import hashlib
import struct
import math


class FlajoletMartin:
    """
    Flajolet-Martin distinct element estimator with multiple hash functions
    and average-of-medians plus phi correction for variance/bias reduction.
    """

    # Flajolet & Martin 1985: E[2^R] ≈ n / phi, so multiply by phi to unbias.
    PHI = 0.77351

    def __init__(self, num_hashes: int = 128, group_size: int = 8):
        """
        Args:
            num_hashes: Total number of hash functions (L). Should be
                        divisible by group_size.
            group_size: Size of each group for the inner median (g).
        """
        self.num_hashes = num_hashes
        self.group_size = group_size
        # Ensure num_hashes is divisible by group_size
        assert num_hashes % group_size == 0, \
            "num_hashes must be divisible by group_size"
        self.num_groups = num_hashes // group_size

        # R[i] = maximum trailing zeros seen by hash function i
        self.max_trailing_zeros = [0] * num_hashes

        # Seeds for hash functions — each gives independent hashing
        self.seeds = list(range(num_hashes))

        self.items_processed = 0

    def _hash(self, item: str, seed: int) -> int:
        """
        Hash an item with a given seed to produce a 64-bit integer.
        Uses SHA-256 with seed prepended for independence.
        """
        data = f"{seed}:{item}".encode("utf-8")
        digest = hashlib.sha256(data).digest()
        # Take first 8 bytes as a 64-bit unsigned integer
        return struct.unpack("<Q", digest[:8])[0]

    @staticmethod
    def _trailing_zeros(value: int) -> int:
        """
        Count the number of trailing zero bits in the binary representation.

        trailing_zeros(0) is defined as 0 (edge case).
        trailing_zeros(0b1000) = 3
        trailing_zeros(0b1010) = 1
        trailing_zeros(0b1) = 0
        """
        if value == 0:
            return 0
        count = 0
        while (value & 1) == 0:
            count += 1
            value >>= 1
        return count

    def add(self, item: str):
        """
        Process a new stream element.

        For each hash function i, compute h_i(item), find trailing zeros r,
        and update R[i] = max(R[i], r).
        """
        for i in range(self.num_hashes):
            h = self._hash(item, self.seeds[i])
            r = self._trailing_zeros(h)
            if r > self.max_trailing_zeros[i]:
                self.max_trailing_zeros[i] = r
        self.items_processed += 1

    @staticmethod
    def _median(values) -> float:
        ordered = sorted(values)
        mid = len(ordered) // 2
        if len(ordered) % 2 == 0:
            return (ordered[mid - 1] + ordered[mid]) / 2.0
        return float(ordered[mid])

    def estimate(self) -> int:
        """
        Estimate the number of distinct elements.

        Method: average-of-medians with phi correction
        1. Divide hash functions into groups of size g.
        2. Within each group, compute 2^R for each hash function,
           then take the MEDIAN (outliers like 2^9 cannot dominate).
        3. Take the AVERAGE across all group medians.
        4. Multiply by phi ≈ 0.77351 (classic FM bias correction).
        """
        if self.items_processed == 0:
            return 0

        group_medians = []
        for g in range(self.num_groups):
            start = g * self.group_size
            end = start + self.group_size
            estimates = [2 ** self.max_trailing_zeros[i]
                         for i in range(start, end)]
            group_medians.append(self._median(estimates))

        avg_of_medians = sum(group_medians) / len(group_medians)
        return int(round(avg_of_medians * self.PHI))

    def memory_usage_bytes(self) -> int:
        """
        Approximate memory usage: one integer per hash function.
        """
        return self.num_hashes * 8  # 8 bytes per int

    def __repr__(self):
        return (f"FlajoletMartin(hashes={self.num_hashes}, "
                f"groups={self.num_groups}, "
                f"processed={self.items_processed}, "
                f"estimate={self.estimate()}, "
                f"memory={self.memory_usage_bytes()} bytes)")
