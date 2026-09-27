#!/usr/bin/env python3
"""
Empirical variance study for FlajoletMartin at small n (and larger n).

Reproduces the observed "23 vs exact 10" regime and compares hash/group
configurations plus optional phi correction (Flajolet-Martin 1985, phi~0.77351).
"""
from __future__ import annotations

import math
import random
import statistics
import sys
from typing import List, Tuple

from flajolet_martin import FlajoletMartin

PHI = 0.77351  # classic FM bias-correction constant


def theoretical_e_2r(n: int, max_k: int = 40) -> float:
    """E[2^R] for a single FM hash given n distinct items (ideal random bits)."""
    expect = 0.0
    for k in range(0, max_k + 1):
        p_ge_k = 1.0 - (1.0 - 2.0 ** (-k)) ** n
        p_ge_k1 = 1.0 - (1.0 - 2.0 ** (-(k + 1))) ** n
        p_eq = p_ge_k - p_ge_k1
        expect += (2.0 ** k) * p_eq
    return expect


def _median(values: List[float]) -> float:
    s = sorted(values)
    m = len(s) // 2
    if len(s) % 2 == 0:
        return (s[m - 1] + s[m]) / 2.0
    return float(s[m])


def estimate_median_of_averages(fm: FlajoletMartin) -> float:
    """Original combiner: average 2^R inside each group, then median of averages."""
    group_averages = []
    for g in range(fm.num_groups):
        start = g * fm.group_size
        end = start + fm.group_size
        estimates = [2 ** fm.max_trailing_zeros[i] for i in range(start, end)]
        group_averages.append(sum(estimates) / len(estimates))
    return _median(group_averages)


def estimate_avg_of_medians(fm: FlajoletMartin) -> float:
    group_medians = []
    for g in range(fm.num_groups):
        start = g * fm.group_size
        end = start + fm.group_size
        estimates = [2 ** fm.max_trailing_zeros[i] for i in range(start, end)]
        group_medians.append(_median(estimates))
    return sum(group_medians) / len(group_medians)


def estimate_2_to_mean_r(fm: FlajoletMartin) -> float:
    """Geometric-style: 2^(mean R) — less sensitive to a single huge R."""
    mean_r = sum(fm.max_trailing_zeros) / len(fm.max_trailing_zeros)
    return 2.0 ** mean_r


def trial(n: int, num_hashes: int, group_size: int, seed_offset: int) -> FlajoletMartin:
    fm = FlajoletMartin(num_hashes=num_hashes, group_size=group_size)
    fm.seeds = [s + seed_offset * 1009 for s in range(num_hashes)]
    hosts = [f"host-{i}.example.net" for i in range(n)]
    for h in hosts:
        fm.add(h)
    return fm


def summarize(name: str, estimates: List[float], exact: int) -> None:
    errs = [abs(e - exact) / exact for e in estimates]
    ratios = [e / exact for e in estimates]
    p50 = statistics.median(estimates)
    mean = statistics.mean(estimates)
    p90 = sorted(estimates)[int(0.9 * (len(estimates) - 1))]
    ge2x = sum(1 for r in ratios if r >= 2.0 or r <= 0.5) / len(ratios)
    print(
        f"{name:42s}  mean={mean:7.2f}  median={p50:7.2f}  "
        f"p90={p90:7.1f}  MAPE={statistics.mean(errs)*100:6.1f}%  "
        f"P(|err|>=2x)={ge2x*100:5.1f}%  "
        f"min={min(estimates):.0f} max={max(estimates):.0f}"
    )


def main() -> None:
    random.seed(42)
    trials = int(sys.argv[1]) if len(sys.argv) > 1 else 80

    print("=== Theoretical single-hash E[2^R] (unbiased random bits) ===")
    for n in (10, 50, 200, 1000):
        e = theoretical_e_2r(n)
        print(f"  n={n:4d}  E[2^R]={e:8.2f}  E[2^R]/n={e/n:5.2f}  "
              f"phi-corrected E[phi*2^R]/n={(PHI * e)/n:5.2f}")
    print()

    configs = [
        ("MoA 64h/8g", 64, 8, "moa", False),
        ("AoM 64h/8g", 64, 8, "aom", False),
        ("AoM 64h/8g + phi", 64, 8, "aom", True),
        ("AoM 128h/16g", 128, 8, "aom", False),
        ("AoM 128h/16g + phi", 128, 8, "aom", True),
        ("2^meanR 64h", 64, 8, "gmean", False),
        ("2^meanR 128h", 128, 8, "gmean", False),
        ("2^meanR/phi 128h", 128, 8, "gmean", True),
    ]

    for n in (10, 200):
        print(f"=== Monte Carlo: n={n} distinct, {trials} trials ===")
        for name, nh, gs, combiner, use_phi in configs:
            ests = []
            for t in range(trials):
                fm = trial(n, nh, gs, seed_offset=t + 1)
                if combiner == "moa":
                    raw = estimate_median_of_averages(fm)
                elif combiner == "aom":
                    raw = estimate_avg_of_medians(fm)
                else:
                    raw = estimate_2_to_mean_r(fm)
                ests.append(raw / PHI if (use_phi and combiner == "gmean") else
                            (raw * PHI if use_phi else raw))
            summarize(name, ests, n)
        print()

    # Reproduce a single "sample window" like the reported 23 vs 10
    print("=== Single-run replica of the reported sample (n=10, current params) ===")
    fm = FlajoletMartin(num_hashes=64, group_size=8)
    for i in range(10):
        fm.add(f"host-{i}.example.net")
    print(f"  estimate={fm.estimate()}  exact=10  R-values={fm.max_trailing_zeros}")
    group_avgs = []
    for g in range(fm.num_groups):
        start = g * fm.group_size
        end = start + fm.group_size
        estimates = [2 ** fm.max_trailing_zeros[i] for i in range(start, end)]
        avg = sum(estimates) / len(estimates)
        group_avgs.append(avg)
        print(f"  group {g}: 2^R={estimates}  avg={avg:.2f}")
    print(f"  group averages (sorted)={sorted(group_avgs)}")
    print(f"  median-of-averages={fm.estimate()}  phi*median={fm.estimate()*PHI:.2f}")


if __name__ == "__main__":
    main()
