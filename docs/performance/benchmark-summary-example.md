# Benchmark Summary Example

This file is a committed example of the human-readable Markdown summary that Stage 16B can generate from a benchmark run.

Use it as a portfolio-friendly reference for:

- what the benchmark measures
- how the sections are organized
- how to read step timings and return codes

The benchmark currently measures three local Docker Compose steps:

1. Go producer runtime
2. Python consumer runtime
3. warehouse loader runtime

It does not measure the entire platform startup cost, cloud latency, or a production-scale deployment. Results are local and depend on the machine, Docker resource limits, and any parallel activity on the host.

## Example Layout

```md
# Benchmark Summary

- Benchmark timestamp: `2026-06-23T13:37:24.000000+00:00`
- Dataset profile: `sample`
- Producer rows: `72`
- Consumer messages: `72`
- Warehouse loader messages: `72`
- Dry run: `False`

## Step Results

| Step | Command | Elapsed seconds | Return code |
| --- | --- | ---: | --- |
| go-producer | `docker compose run --rm go-producer` | 2.315 | 0 |
| python-consumer | `docker compose run --rm python-consumer` | 3.842 | 0 |
| warehouse-loader | `docker compose run --rm warehouse-loader` | 4.127 | 0 |

**Total elapsed time:** `10.284` seconds

## Interpretation

- These timings show how long the local Docker-based producer, consumer, and loader steps took for this dataset profile.
- Use repeated runs on the same machine and Docker resource settings for fair comparisons between sample, medium, and full profiles.

## Note

- Results depend on the local machine, active Docker resource limits, and any other workload running at the same time.
- Treat these numbers as environment-specific local measurements rather than portable absolute performance claims.
```

## How to Read It

- `Dataset profile` tells you whether the run targeted `sample`, `medium`, or `full`.
- `Producer rows`, `Consumer messages`, and `Warehouse loader messages` show the intended size of the bounded run.
- `Elapsed seconds` gives the measured time for each individual step.
- `Total elapsed time` lets you compare whole benchmark runs at a glance.
- `Return code` helps distinguish a valid timing run from a failed one.
- `Interpretation` adds short human context so the benchmark can be understood without opening the raw JSON file.

## JSON vs Markdown

- The JSON result file is the machine-readable source artifact.
- The generated Markdown summary is the human-readable local report derived from that JSON-style result data.
- This committed example file is documentation only. It is safe to keep in git because it is a template, not a generated local benchmark artifact.
