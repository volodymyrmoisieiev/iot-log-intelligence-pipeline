# Stage 23 Concurrent Streaming E2E Runtime

## What Stage 23 Adds

Stage 23 extends the local E2E smoke-test helper with an opt-in concurrent runtime mode:

- `--concurrent-pipeline`

This mode keeps the existing bounded local validation pattern, isolated Kafka topics, and PostgreSQL delta verification, but changes the runtime order so the downstream streaming services are already waiting when the producer starts.

What Stage 23 adds:

- a preserved default sequential mode
- an opt-in concurrent runtime mode for the controlled profile pipeline
- reusable concurrent process runner helpers based on `subprocess.Popen`
- concurrent JSON reporting with per-process status, return code, duration, and orchestrator-termination details
- validated local sample and medium concurrent runs

## Why Sequential Mode Still Exists

Sequential mode remains valuable because it is simpler to debug:

- the producer completes before the consumer starts
- the consumer completes before the warehouse loader starts
- failures are easier to isolate one stage at a time
- it remains the safest default for routine local checks

Stage 23 does not replace sequential mode. It adds concurrent mode as an intentional option for cases where a more streaming-like local runtime is useful.

## Why Concurrent Mode Matters

The real pipeline is conceptually streaming:

- producer writes raw events
- consumer transforms and republishes them
- warehouse loader consumes transformed events and writes them to PostgreSQL

Running those stages concurrently is a closer local approximation of that behavior. It helps validate:

- end-to-end coordination across overlapping stages
- process lifecycle handling
- safe termination if one concurrent process fails
- reporting that reflects real overlapping wall-clock runtime

## Old Sequential Flow

The controlled sequential profile runtime works like this:

1. start `kafka`, `kafka-init`, and `postgres`
2. create isolated raw, processed, and invalid Kafka topics
3. capture pre-run PostgreSQL counts
4. run Go producer
5. run Python consumer
6. run warehouse loader
7. capture post-run PostgreSQL counts
8. verify `processed_delta + invalid_delta == expected_rows`

This flow remains the default behavior whenever `--concurrent-pipeline` is not provided.

## New Concurrent Flow

The controlled concurrent profile runtime works like this:

1. start `kafka`, `kafka-init`, and `postgres`
2. create isolated raw, processed, and invalid Kafka topics
3. capture pre-run PostgreSQL counts
4. build `go-producer`, `python-consumer`, and `warehouse-loader`
5. start Python consumer first
6. start warehouse loader second
7. wait for a short warm-up
8. start Go producer last
9. wait for all three processes
10. if one required process fails, terminate the remaining running processes
11. capture post-run PostgreSQL counts
12. verify `processed_delta + invalid_delta == expected_rows`

Concurrent mode also raises the runtime idle timeout for the consumer and warehouse loader to `30` seconds so they do not exit too early while waiting for the producer.

## Command Examples

Sequential sample:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --run-profile-pipeline --output-json docs/e2e-smoke-test-local.json
```

Concurrent sample:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile sample --max-rows 1000 --run-profile-pipeline --concurrent-pipeline --output-json docs/e2e-smoke-test-local.json
```

Concurrent medium `10000`:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile medium --max-rows 10000 --run-profile-pipeline --concurrent-pipeline --output-json docs/e2e-smoke-test-local.json
```

Optional concurrent full `100000`:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile full --max-rows 100000 --run-profile-pipeline --concurrent-pipeline --allow-full-run --output-json docs/e2e-smoke-test-local.json
```

For live manual concurrent output, prefer:

```powershell
.\.venv-observability\Scripts\python.exe .\scripts\run_local_e2e_smoke_test.py --profile medium --max-rows 10000 --run-profile-pipeline --concurrent-pipeline --stream-output --progress-mode log --output-json docs/e2e-smoke-test-local.json
```

That keeps the terminal readable while three processes are active at the same time.

## Failure Handling

Concurrent failure handling is intentionally simple and safe:

- all three runtime processes are treated as required
- if one process returns a non-zero exit code, the helper terminates the others
- termination first uses `process.terminate()`
- if that is not enough, the helper falls back to `process.kill()`
- the JSON summary records whether a process was terminated by the orchestrator and why

This makes failure behavior explicit instead of allowing the remaining processes to continue indefinitely after a critical runtime error.

## JSON Metadata

Concurrent JSON output records:

- `pipeline_execution_mode`
- `stage_durations_seconds`
- `profile_pipeline_progress`
- `profile_pipeline_concurrent_runtime`

The dedicated `profile_pipeline_concurrent_runtime` section records:

- overall concurrent runtime status
- summary details
- total concurrent wall-clock duration
- failed process names if any
- per-process status
- per-process return code
- per-process duration
- `terminated_by_orchestrator`
- `termination_reason`

The lower-level `checks` array still includes the detailed per-process check entries for:

- `profile_pipeline_consumer`
- `profile_pipeline_warehouse_loader`
- `profile_pipeline_producer`

Sequential JSON remains unchanged except for the existing execution-mode field:

- `pipeline_execution_mode=sequential`

## Validated Results

Validated Stage 23 results:

- sequential sample passed with `processed_delta=72`, `invalid_delta=0`, `total_delta=72`
- concurrent sample passed with `processed_delta=72`, `invalid_delta=0`, `total_delta=72`
- concurrent medium `10000` passed with `processed_delta=10000`, `invalid_delta=0`, `total_delta=10000`

Validated concurrent medium reporting:

- `total_wall_clock_duration_seconds=24.306`
- producer return code `0`
- consumer return code `0`
- warehouse-loader return code `0`
- no process was terminated by the orchestrator

## Known Limitations

- concurrent mode is still a controlled local validation path, not a production orchestrator
- `docker compose run` process startup overhead is still part of the measured wall-clock duration
- the local E2E helper still uses bounded message counts and local Docker services
- live progress bars from multiple concurrent processes can become noisy, which is why `--stream-output --progress-mode log` is recommended
- the optional full `100000` concurrent run is heavier and remains an intentional manual path rather than a required default validation step

## Future Improvements

Useful follow-up ideas after Stage 23 include:

- optional larger concurrent validation runs with summarized comparison output
- clearer concurrent failure classification for timeout and idle-timeout cases
- automated comparison of sequential versus concurrent wall-clock results
- targeted cleanup helpers for isolated topics and repeated local benchmark-style runs
