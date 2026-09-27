"""
Bloom Filter — Probabilistic Set Membership Data Structure
==========================================================

A Bloom filter is a space-efficient probabilistic data structure that tests
whether an element is a member of a set. False positive matches are possible,
but false negatives are not: a query returns either "possibly in set" or
"definitely not in set".

HOW IT WORKS (Mining of Massive Datasets, Ch. 4):
  1. We maintain a bit array B of size m, initially all zeros.
  2. We use k independent hash functions h1, h2, ..., hk, each mapping
     items to positions in [0, m-1].
  3. To ADD an item x: set B[h1(x)] = B[h2(x)] = ... = B[hk(x)] = 1.
  4. To QUERY an item x: check if B[h1(x)] AND B[h2(x)] AND ... AND B[hk(x)]
     are all 1. If yes → "possibly in set" (may be false positive).
     If any is 0 → "definitely not in set".

OPTIMAL PARAMETERS:
  Given:
    n = expected number of elements
    p = desired false positive probability
  Then:
    m = -(n * ln(p)) / (ln(2))^2     (optimal bit array size)
    k = (m / n) * ln(2)              (optimal number of hash functions)

FALSE POSITIVE RATE:
  After inserting n elements, the probability of a false positive is
  approximately (1 - e^(-kn/m))^k.

WHY USE APPROXIMATE ALGORITHMS?
  In streaming scenarios with millions of unique items, storing an exact
  set would require O(n) memory. A Bloom filter uses only O(1) memory
  (fixed m bits) regardless of how many items are inserted, at the cost
  of a small, controllable false positive rate.
"""

import math
import hashlib
import struct


class BloomFilter:
    """
    Bloom Filter with configurable false-positive rate.

    Uses multiple independent hash functions derived from MD5 with
    different seeds to map items to bit positions.
    """

    def __init__(self, expected_elements: int = 10000, fp_rate: float = 0.01):
        """
        Initialize the Bloom filter.

        Args:
            expected_elements (n): Expected number of distinct elements
            fp_rate (p): Target false positive probability (e.g. 0.01 = 1%)
        """
        self.expected_elements = expected_elements
        self.fp_rate = fp_rate

        # Calculate optimal bit array size: m = -(n * ln(p)) / (ln(2))^2
        self.size = self._optimal_size(expected_elements, fp_rate)

        # Calculate optimal number of hash functions: k = (m/n) * ln(2)
        self.num_hashes = self._optimal_hash_count(self.size, expected_elements)

        # Initialize bit array as a bytearray (each byte holds 8 bits)
        # This is more memory-efficient than a list of booleans
        self._bit_array = bytearray(math.ceil(self.size / 8))

        # Counters for monitoring
        self.items_added = 0

    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        """
        Compute optimal bit array size m.
        m = -(n * ln(p)) / (ln(2))^2
        """
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return max(int(math.ceil(m)), 64)  # minimum 64 bits

    @staticmethod
    def _optimal_hash_count(m: int, n: int) -> int:
        """
        Compute optimal number of hash functions k.
        k = (m / n) * ln(2)
        """
        k = (m / n) * math.log(2)
        return max(int(round(k)), 1)  # minimum 1 hash

    def _get_hash_positions(self, item: str) -> list[int]:
        """
        Generate k hash positions for an item using the double-hashing technique.

        We compute two base hashes (h1, h2) from MD5 and derive k positions as:
            position_i = (h1 + i * h2) mod m

        This is a well-known technique (Kirsch & Mitzenmacker, 2006) that
        gives the same theoretical guarantees as k independent hash functions.
        """
        # Convert item to bytes and hash with MD5 (128 bits = two 64-bit ints)
        digest = hashlib.md5(item.encode("utf-8")).digest()
        h1, h2 = struct.unpack("<QQ", digest)  # two 64-bit unsigned ints

        positions = []
        for i in range(self.num_hashes):
            pos = (h1 + i * h2) % self.size
            positions.append(pos)
        return positions

    def _set_bit(self, position: int):
        """Set a single bit in the bit array."""
        byte_idx = position // 8
        bit_idx = position % 8
        self._bit_array[byte_idx] |= (1 << bit_idx)

    def _get_bit(self, position: int) -> bool:
        """Get a single bit from the bit array."""
        byte_idx = position // 8
        bit_idx = position % 8
        return bool(self._bit_array[byte_idx] & (1 << bit_idx))

    def add(self, item: str):
        """
        Add an item to the Bloom filter.
        Sets k bit positions to 1.
        """
        for pos in self._get_hash_positions(item):
            self._set_bit(pos)
        self.items_added += 1

    def might_contain(self, item: str) -> bool:
        """
        Test if an item might be in the set.

        Returns:
            True  → item is POSSIBLY in the set (may be false positive)
            False → item is DEFINITELY NOT in the set
        """
        return all(self._get_bit(pos) for pos in self._get_hash_positions(item))

    def add_and_check(self, item: str) -> bool:
        """
        Check if item was seen before, then add it.

        Returns:
            True  → item was SEEN BEFORE (possibly false positive)
            False → item is NEW (definitely not seen before)
        """
        was_present = self.might_contain(item)
        if not was_present:
            self.add(item)
        return was_present

    def estimated_fp_rate(self) -> float:
        """
        Compute the estimated false positive rate given current state.
        FP rate ≈ (1 - e^(-k*n/m))^k
        """
        if self.items_added == 0:
            return 0.0
        exponent = -self.num_hashes * self.items_added / self.size
        return (1 - math.exp(exponent)) ** self.num_hashes

    def memory_usage_bytes(self) -> int:
        """Return approximate memory usage of the bit array in bytes."""
        return len(self._bit_array)

    def __repr__(self) -> str:
        return (f"BloomFilter(size={self.size} bits, "
                f"hashes={self.num_hashes}, "
                f"items={self.items_added}, "
                f"est_fp_rate={self.estimated_fp_rate():.4f}, "
                f"memory={self.memory_usage_bytes()} bytes)")
