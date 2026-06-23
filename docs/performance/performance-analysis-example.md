# Performance Analysis Example

This file is a committed example of the Stage 16C benchmark analysis output.

Use it to understand:

- how multiple benchmark JSON files are summarized together
- how bottleneck interpretation is presented
- how to read aggregate timing shares and per-run comparisons

The analysis tool reads JSON artifacts from `scripts/run_performance_benchmark.py` and turns them into a human-readable report. It is designed for local benchmarking, so any conclusions still depend on the host machine, Docker resource limits, and whether the runs were real or only `--dry-run`.

## Example Layout

```md
# Performance Analysis

- Input path: `docs/performance/results`
- Profile filter: `all`
- Valid benchmark runs: `3`
- Dry-run results: `1`
- Real benchmark runs: `2`
- Profiles included: `sample`, `medium`

## Rows and Message Caps

- rows=`72`, consumer_messages=`72`, loader_messages=`72` across `1` run(s)
- rows=`1000`, consumer_messages=`1000`, loader_messages=`1000` across `2` run(s)

## Aggregate Step Breakdown

| Step | Elapsed seconds | Percent share |
| --- | --- | --- |
| go-producer | 5.102 | 29.11% |
| python-consumer | 6.488 | 37.02% |
| warehouse-loader | 5.935 | 33.87% |

## Per-Run Summary

| File | Timestamp | Profile | Rows | Consumer messages | Loader messages | Total seconds | Throughput rows/sec | Slowest step |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| benchmark_sample_72_rows_1.json | 2026-06-23T13:37:24+00:00 | sample | 72 | 72 | 72 | 0.000 | n/a | not meaningful (dry-run) |
| benchmark_medium_1000_rows_1.json | 2026-06-23T14:10:00+00:00 | medium | 1000 | 1000 | 1000 | 8.772 | 113.99 | python-consumer |
| benchmark_medium_1000_rows_2.json | 2026-06-23T14:18:00+00:00 | medium | 1000 | 1000 | 1000 | 8.753 | 114.25 | warehouse-loader |

## Fastest and Slowest Run

- Fastest run: `benchmark_sample_72_rows_1.json` at `0.000` seconds
- Slowest run: `benchmark_medium_1000_rows_1.json` at `8.772` seconds

## Average Total Time by Profile

| Profile | Run count | Dry-run count | Average total seconds |
| --- | --- | --- | --- |
| sample | 1 | 1 | 0.000 |
| medium | 2 | 0 | 8.763 |

## Bottleneck Interpretation

- Dry-run benchmark results were detected, so their timings are not meaningful for real performance analysis.
- The dominant bottleneck across real runs was `python-consumer`, and the most frequent slowest step per run was `python-consumer` (1/2 runs).
- Focus on validation cost, payload deserialization, Kafka polling behavior, and consumer batch sizing if the Python consumer remains the slowest stage.
```

## How to Read the Analysis

- `Valid benchmark runs` tells you how many JSON files were usable after filtering and after skipping malformed files.
- `Dry-run results` warns that some runs are metadata-only checks and should not be treated as real performance evidence.
- `Aggregate Step Breakdown` shows where total recorded time is concentrated across the analyzed run set.
- `Per-Run Summary` helps compare dataset profile, row caps, throughput, and the slowest step for each individual benchmark file.
- `Average Total Time by Profile` helps compare `sample`, `medium`, and `full` behavior across repeated runs.
- `Bottleneck Interpretation` turns the raw timing pattern into optimization guidance for producer, consumer, or warehouse-loader work.

## Analysis vs Raw Files

- Raw JSON benchmark files are machine-readable artifacts and stay ignored by git.
- Optional local Markdown analysis files such as `docs/performance/performance-analysis-local.md` are also local artifacts and should normally stay out of commits.
- This committed example file is documentation only, so it is safe to keep under version control.
