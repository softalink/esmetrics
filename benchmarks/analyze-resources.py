#!/usr/bin/env python3
"""Summarize resource samples recorded by bench-monitored.sh / bench-monitored.ps1.

Usage: ./analyze-resources.py <results-dir> [<results-dir> ...]

Per phase (load, query) reports: wall duration, peak CPU%% over 1s windows,
average CPU%%, total CPU seconds, peak resident memory; plus the lifetime
peak (VmHWM / PeakWorkingSet64) and the fully-merged storage size.

Sample formats (auto-detected):
  Linux   : epoch_sec(float) cpu_ticks vmrss_kb vmhwm_kb
  Windows : epoch_ms(int) cpu_total_sec workingset_bytes peakworkingset_bytes
"""
import bisect
import os
import sys


def parse_dir(d):
    with open(os.path.join(d, "samples.txt")) as f:
        raw = [ln.replace(",", ".").split() for ln in f if len(ln.split()) == 4]
    windows = float(raw[0][0]) > 1e11  # epoch in ms => Windows
    samples = []
    for p in raw:
        if windows:
            t, cpu = int(p[0]) / 1000.0, float(p[1])
            rss, peak = int(p[2]) / 1024.0, int(p[3]) / 1024.0
        else:
            hz = os.sysconf("SC_CLK_TCK")
            t, cpu = float(p[0]), int(p[1]) / hz
            rss, peak = float(p[2]), float(p[3])
        samples.append((t, cpu, rss, peak))  # sec, cpu-sec, kb, kb
    phases = {}
    with open(os.path.join(d, "phases.txt")) as f:
        for ln in f:
            name, t = ln.split()
            phases[name] = int(t) / 1000.0 if windows else float(t)
    storage = {}
    with open(os.path.join(d, "storage.txt")) as f:
        for ln in f:
            name, b = ln.split()
            storage[name] = int(b)
    return samples, phases, storage


def phase_stats(samples, start, end):
    win = [s for s in samples if start <= s[0] <= end]
    if len(win) < 2:
        return None
    dur = win[-1][0] - win[0][0]
    cpu_sec = win[-1][1] - win[0][1]
    times = [s[0] for s in win]
    peak_cpu = 0.0
    for s0 in win:
        j = bisect.bisect_left(times, s0[0] + 1.0)
        if j >= len(win):
            break
        s1 = win[j]
        peak_cpu = max(peak_cpu, (s1[1] - s0[1]) / (s1[0] - s0[0]) * 100.0)
    return {
        "dur": dur,
        "peak_cpu": peak_cpu,
        "avg_cpu": cpu_sec / dur * 100.0 if dur > 0 else 0.0,
        "cpu_sec": cpu_sec,
        "peak_rss_kb": max(s[2] for s in win),
    }


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    for d in sys.argv[1:]:
        samples, phases, storage = parse_dir(d)
        print(f"== {d} ==")
        for pname, (a, b) in {"load": ("load_start", "load_end"),
                              "query": ("query_start", "query_end")}.items():
            st = phase_stats(samples, phases[a], phases[b])
            if st is None:
                print(f"  {pname:6} (phase too short to sample)")
                continue
            print(f"  {pname:6} dur {st['dur']:6.1f}s  peakCPU {st['peak_cpu']:6.1f}%  "
                  f"avgCPU {st['avg_cpu']:6.1f}%  cpuSec {st['cpu_sec']:7.1f}  "
                  f"peakRSS {st['peak_rss_kb'] / 1024:6.0f} MiB")
        print(f"  lifetime peak RSS: {max(s[3] for s in samples) / 1024:.0f} MiB   "
              f"storage post-merge: {storage['post_merge'] / 1e6:.2f} MB")


if __name__ == "__main__":
    main()
