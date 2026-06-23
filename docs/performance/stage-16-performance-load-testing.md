# Stage 16 Performance / Load Testing

## What Stage 16 Adds

Stage 16 adds a lightweight local performance and load-testing foundation on top of the existing dataset-profile work from Stage 15.

The stage includes:

- benchmark execution with `scripts/run_performance_benchmark.py`
- optional Markdown benchmark summaries for individual runs
- aggregate benchmark analysis with `scripts/analyze_performance_results.py`
- committed example files that explain how to read benchmark output

Stage 16 does not change producer, consumer, warehouse-loader, Airflow, dbt, Spark, MinIO, or Terraform runtime logic. It adds measurement and documentation around the existing local pipeline behavior.

## Why This Matters After Dataset Profiles

Stage 15 introduced `sample`, `medium`, and `full` dataset modes so the same pipeline can be exercised at different sizes.

Stage 16 makes those profiles more useful by adding a repeatable way to answer questions such as:

- how long does the local pipeline take for a small validation run
- which step becomes the slowest as the batch size grows
- how should benchmark output be presented clearly in a portfolio context

That turns dataset profiles from simple input modes into a practical local testing workflow.

## Benchmark Flow

The Stage 16 benchmark flow is intentionally simple:

1. choose a dataset profile and row or message caps
2. run the benchmark helper
3. capture a machine-readable JSON result
4. optionally generate a human-readable Markdown summary
5. analyze one or many JSON results to identify timing patterns and bottlenecks

The benchmark helper measures three existing pipeline steps:

1. Go producer
2. Python consumer
3. warehouse loader

## Example Commands

### Sample mode benchmark

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_performance_benchmark.py --profile sample --rows 72 --summary-md docs/performance/benchmark-summary-local.md
```

### Medium mode benchmark

Generate the medium dataset first if it does not exist:

```powershell
python .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000 --overwrite
```

Then run a bounded medium benchmark:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_performance_benchmark.py --profile medium --rows 1000 --consumer-messages 1000 --loader-messages 1000 --summary-md docs/performance/benchmark-summary-local.md
```

### Analyzer command

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\analyze_performance_results.py --input docs/performance/results --output-md docs/performance/performance-analysis-local.md
```

## What the Output Files Are For

### JSON reports

JSON benchmark reports are the machine-readable source artifacts.

They are useful for:

- preserving exact timing values
- comparing repeated runs
- feeding the analysis helper
- keeping raw evidence separate from human-written documentation

### Markdown summaries

Markdown benchmark summaries are human-readable run reports for one benchmark execution.

They are useful for:

- quick review of one run
- portfolio screenshots or selected evidence
- showing timestamps, rows, return codes, and step timings without opening JSON

## How to Interpret Bottlenecks

The analysis helper highlights which step dominates total measured time.

Use the interpretation this way:

- if `go-producer` is slowest, look at Kafka publish rate, send delay, batching, and producer configuration
- if `python-consumer` is slowest, look at validation cost, deserialization, Kafka polling behavior, and batch handling
- if `warehouse-loader` is slowest, look at insert strategy, batching, indexes, and transaction handling
- if dry-run files dominate the result set, do not treat timings as real performance evidence

The analyzer is meant to point your attention toward likely pressure areas, not to replace deeper profiling.

## Limitations of Local Docker Benchmarks

Stage 16 benchmark numbers are local environment measurements, not production guarantees.

Important limitations:

- results depend on the host machine
- Docker CPU and memory allocation affect timings
- background system load can skew comparisons
- dry-run results validate workflow only, not real throughput
- local single-node behavior does not represent cloud-scale concurrency

Because of that, Stage 16 should be treated as a practical local benchmarking foundation rather than a final production performance study.

## Portfolio Value

For a Data Engineering portfolio, Stage 16 demonstrates that the project does more than process data correctly.

It shows that the repository also includes:

- structured benchmark execution
- environment-aware summary reporting
- repeatable bottleneck analysis
- clear handling of local artifacts versus committed documentation

That helps present the pipeline as something that can be measured, explained, and improved, not only run once for a demo.
