# Stage 20 CI Quality Gates

## What Stage 20 adds

Stage 20 finalizes the repository's first structured CI/CD quality-gate foundation for pull requests and protected-branch updates.

Across Stages 20A through 20E, the repository now includes:

- a base GitHub Actions CI workflow for repository structure and lightweight validation
- a dedicated Terraform validation workflow for the AWS orchestration foundation
- a dedicated Python and Airflow validation workflow for important scripts and DAG files
- a pull request template for safer review handoff
- a release checklist for cleaner branch and merge discipline
- visible CI quality badges in the README

## Why CI/CD quality gates matter

Quality gates help catch obvious regressions before merge, make review expectations clearer, and show that the project is managed with production-minded engineering habits rather than ad hoc manual verification alone.

For a portfolio repository, this matters in three ways:

- it demonstrates repeatable validation habits
- it reduces reviewer uncertainty about what was checked
- it makes the project easier for another engineer to understand and trust

## GitHub Actions workflows in Stage 20

### 1. Base CI

Workflow file:

- `.github/workflows/ci.yml`

What it validates:

- repository checkout
- basic repository metadata output
- expected project-structure checks
- `docker compose config`
- lightweight Python syntax compilation for selected entry points when those files exist

### 2. Terraform validation

Workflow file:

- `.github/workflows/terraform-validate.yml`

What it validates:

- repository checkout
- Terraform CLI setup
- `terraform fmt -check`
- `terraform init -backend=false`
- `terraform validate`

Why it is safe:

- it does not use AWS credentials
- it does not connect to backend state
- it does not run `terraform apply`
- it does not create cloud resources

### 3. Python and Airflow validation

Workflow file:

- `.github/workflows/python-airflow-validate.yml`

What it validates:

- Python 3.11 setup
- `py_compile` checks for important scripts under `scripts/`
- `py_compile` checks for the Lambda handler under `aws/lambda/`
- `py_compile` checks for observability Python entry points
- `py_compile` checks for DAG files under `airflow/dags/`
- lightweight DAG file listing for easier debugging

Why it is safe:

- it does not run full DAG execution
- it does not start the full Kafka/PostgreSQL/Spark pipeline
- it does not require secrets
- it does not require AWS credentials

## What the PR template adds

The pull request template at `.github/pull_request_template.md` gives every PR a consistent structure for:

- summary
- included changes
- validation evidence
- risk and impact review
- notes for reviewers
- a pre-merge checklist for CI, docs, artifact hygiene, and secret safety

## What the release checklist adds

The release checklist at `docs/release-checklist.md` documents the expected branch flow and cleanup process:

- `feature/* -> develop` via pull request
- `Squash and merge` for feature work into `develop`
- `develop -> main` via release pull request
- `Merge pull request` for release promotion into `main`
- local branch refresh and cleanup after merge

This keeps merge behavior predictable and makes the repository feel more like a maintained engineering project than a loose collection of code drops.

## What is intentionally not run in CI

Stage 20 intentionally keeps CI cost-safe and lightweight. The workflows do not run:

- the full Kafka/PostgreSQL pipeline
- full Airflow DAG execution
- the full dataset
- AWS deployment
- `terraform apply`

This is deliberate. Full end-to-end runtime validation still belongs to local manual or demo-oriented verification for this repository stage.

## Equivalent local checks

### Docker Compose

```powershell
docker compose config
```

### Python syntax validation

```powershell
.\.venv-observability\Scripts\python.exe -m py_compile .\scripts\run_anomaly_detection.py
.\.venv-observability\Scripts\python.exe -m py_compile .\scripts\validate_data_contract.py
.\.venv-observability\Scripts\python.exe -m py_compile .\scripts\run_performance_benchmark.py
.\.venv-observability\Scripts\python.exe -m py_compile .\scripts\analyze_performance_results.py
.\.venv-observability\Scripts\python.exe -m py_compile .\scripts\create_dataset_profile.py
.\.venv-observability\Scripts\python.exe -m py_compile .\aws\lambda\iot_metadata_validator\handler.py
.\.venv-observability\Scripts\python.exe -m py_compile .\observability\write_pipeline_observability.py
.\.venv-observability\Scripts\python.exe -m py_compile .\airflow\dags\iot_local_pipeline_dag.py
.\.venv-observability\Scripts\python.exe -m py_compile .\airflow\dags\iot_pipeline_smoke_dag.py
```

### Terraform validation

```powershell
terraform -chdir=infra/aws-orchestration fmt -check
terraform -chdir=infra/aws-orchestration init -backend=false
terraform -chdir=infra/aws-orchestration validate
```

### Optional local Airflow checks

```powershell
docker compose run --rm airflow-webserver python -m py_compile /opt/airflow/dags/iot_local_pipeline_dag.py
docker compose run --rm airflow-webserver airflow dags list
docker compose run --rm airflow-webserver airflow tasks list iot_local_pipeline_dag
```

## How this improves project and portfolio quality

Stage 20 makes the repository stronger in several visible ways:

- reviewers can see automated checks directly from the README
- contributors get a repeatable PR and release flow
- infrastructure, Python, and DAG validation are separated into understandable quality gates
- the project communicates engineering discipline, not just implementation volume

## Future improvements

- dbt CI checks
- data contract CI checks
- a local end-to-end smoke test
- GitHub Actions artifacts for validation outputs
- an optional AWS plan workflow with explicit safety boundaries
