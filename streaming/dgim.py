"""
DGIM Algorithm — Approximate Counting of 1s in a Sliding Window
================================================================

The DGIM (Datar-Gionis-Indyk-Motwani) algorithm maintains an approximate
count of 1-bits in the last N positions of a binary stream, using only
O(log²N) memory instead of O(N) for exact counting.

HOW IT WORKS (Mining of Massive Datasets, Ch. 4.6):

1. BUCKETS: We maintain a list of "buckets", each with:
   - A timestamp (position in the stream when the bucket's rightmost 1-bit arrived)
   - A size (always a power of 2: 1, 2, 4, 8, ...)
   The size represents how many 1-bits the bucket "covers".

2. RULES:
   - Buckets are ordered by timestamp (most recent first).
   - For each size s, there are either 1 or 2 buckets of that size.
   - If adding a new bit creates 3 buckets of size s, we MERGE the two
     oldest into one bucket of size 2s (and propagate if needed).

3. ADDING A BIT:
   - Drop any buckets whose timestamp is older than N positions ago.
   - If the new bit is 0: do nothing else.
   - If the new bit is 1: create a new bucket of size 1 with the current
     timestamp. Then check the merging rule and merge if needed.

4. QUERYING THE COUNT:
   - Sum up all bucket sizes, BUT the oldest bucket's size is halved
     (since we don't know exactly how many of its 1-bits are within
     the window).
   - This gives an estimate with at most 50% error on the oldest bucket,
     which translates to at most 50% overall relative error.

WHY APPROXIMATE?
  In a streaming context with potentially billions of items, storing the
  exact last N bits requires O(N) memory. DGIM uses only O(log²N) memory
  by summarizing older parts of the window with increasingly coarse
  granularity. The trade-off is a bounded approximation error.

ACCURACY:
  The maximum error is at most 50% of the oldest bucket's size.
  In practice, the error is much smaller. The guarantee is:
    |estimate - true_count| ≤ (true_count) / 2
  More precisely, the error is at most the size of the largest bucket / 2.
"""


class Bucket:
    """A DGIM bucket with a timestamp and size (power of 2)."""
    __slots__ = ['timestamp', 'size']

    def __init__(self, timestamp: int, size: int):
        self.timestamp = timestamp
        self.size = size

    def __repr__(self):
        return f"Bucket(ts={self.timestamp}, size={self.size})"


class DGIM:
    """
    DGIM algorithm for approximate counting of 1-bits in the last N
    positions of a binary stream.

    Usage:
        dgim = DGIM(window_size=1000)

        # For each incoming request, add 1 if it's a 5xx error, 0 otherwise
        dgim.add_bit(1 if status >= 500 else 0)

        # Query approximate count of errors in last 1000 requests
        approx_count = dgim.count()
    """

    def __init__(self, window_size: int = 1000):
        """
        Args:
            window_size (N): Number of most recent positions to track.
        """
        self.window_size = window_size
        self.current_timestamp = 0
        # Buckets stored in order: most recent first
        self.buckets: list[Bucket] = []

    def add_bit(self, bit: int):
        """
        Process the next bit in the stream.

        Step 1: Advance timestamp
        Step 2: Remove expired buckets (older than window_size ago)
        Step 3: If bit is 1, create new size-1 bucket and merge if needed
        """
        # Step 1: Advance the stream position
        self.current_timestamp += 1

        # Step 2: Drop expired buckets
        # A bucket is expired if its timestamp is more than window_size
        # positions behind the current timestamp
        cutoff = self.current_timestamp - self.window_size
        self.buckets = [b for b in self.buckets if b.timestamp > cutoff]

        # Step 3: If the bit is 0, nothing to do
        if bit == 0:
            return

        # Bit is 1: create a new bucket of size 1
        new_bucket = Bucket(timestamp=self.current_timestamp, size=1)
        # Insert at the front (most recent position)
        self.buckets.insert(0, new_bucket)

        # Merge rule: ensure at most 2 buckets of each size
        # Walk from the end (oldest) and merge when 3 buckets of same size exist
        self._merge_buckets()

    def _merge_buckets(self):
        """
        Enforce the DGIM invariant: at most 2 buckets of each size.

        We scan from the end (oldest buckets) because merging propagates
        toward larger sizes. When we find 3 consecutive buckets of the
        same size, we merge the two oldest into one bucket of double size.
        """
        # It's easier to work from the end of the list (oldest buckets)
        i = len(self.buckets) - 1
        while i >= 2:
            # Check if buckets[i], buckets[i-1], buckets[i-2] all have the same size
            if (self.buckets[i].size == self.buckets[i - 1].size ==
                    self.buckets[i - 2].size):
                # Merge the two oldest (i and i-1) into one of double size
                # The merged bucket keeps the MORE RECENT timestamp of the two
                merged_ts = self.buckets[i - 1].timestamp
                merged_size = self.buckets[i].size * 2
                # Remove the two oldest
                del self.buckets[i]
                del self.buckets[i - 1]
                # Insert merged bucket at position i-1
                self.buckets.insert(i - 1, Bucket(merged_ts, merged_size))
                # After merging, continue checking (the merge may cascade)
                i = len(self.buckets) - 1
            else:
                i -= 1

    def count(self) -> int:
        """
        Return the approximate count of 1-bits in the last N positions.

        Sum all bucket sizes, but HALVE the oldest bucket's contribution
        (since we don't know how much of it falls within the window).

        Returns:
            Approximate count of 1s in the sliding window.
        """
        if not self.buckets:
            return 0

        total = 0
        for i, bucket in enumerate(self.buckets):
            if i == len(self.buckets) - 1:
                # Oldest bucket: count only half its size
                total += bucket.size // 2
            else:
                total += bucket.size
        return total

    def exact_bucket_sum(self) -> int:
        """
        Return the raw sum of all bucket sizes (without halving the oldest).
        Useful for comparison / debugging.
        """
        return sum(b.size for b in self.buckets)

    def num_buckets(self) -> int:
        """Return the current number of buckets."""
        return len(self.buckets)

    def memory_usage(self) -> str:
        """
        Show memory usage: O(log²N) buckets.
        Each bucket stores just 2 integers (timestamp, size).
        """
        n_buckets = len(self.buckets)
        return f"{n_buckets} buckets (~{n_buckets * 16} bytes)"

    def __repr__(self):
        return (f"DGIM(window={self.window_size}, "
                f"timestamp={self.current_timestamp}, "
                f"buckets={self.num_buckets()}, "
                f"approx_count={self.count()})")
