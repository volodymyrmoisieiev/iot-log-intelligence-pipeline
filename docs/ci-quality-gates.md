# Stage 20 CI Quality Gates

## What Stage 20 adds

Stage 20 starts the repository's GitHub Actions quality-gate path. Stage 20A introduces a safe, fast CI foundation that runs on pull requests targeting `develop` and `main`, plus direct pushes to those branches. Stage 20B adds a separate Terraform validation workflow for the AWS orchestration foundation under `infra/aws-orchestration/`.

This stage is intentionally conservative. It gives the project a repeatable automated checkpoint for structural problems and obvious syntax regressions without trying to run the full local platform inside GitHub-hosted runners.

## What runs in Stage 20A

The workflow lives at `.github/workflows/ci.yml` and currently runs these checks:

- repository checkout
- basic repository metadata output for easier debugging
- required project-structure verification for:
  - `airflow/`
  - `dbt/`
  - `scripts/`
  - `infra/aws-orchestration/`
  - `aws/lambda/iot_metadata_validator/`
  - `docs/`
- Docker Compose configuration validation through `docker compose config`
- lightweight Python syntax compilation for selected entry points when those files exist:
  - `scripts/run_anomaly_detection.py`
  - `scripts/validate_data_contract.py`
  - `scripts/run_performance_benchmark.py`
  - `scripts/analyze_performance_results.py`
  - `aws/lambda/iot_metadata_validator/handler.py`
  - `airflow/dags/iot_local_pipeline_dag.py`

If one of the expected Python files is absent in a future refactor, the workflow skips that file instead of failing purely because the file list changed.

## Why this helps pull request quality

Stage 20A catches several common failure modes early:

- accidental repository-structure breakage
- broken `docker-compose.yml` edits
- Python syntax regressions in important orchestration and utility entry points
- missing key folders required by the current local and AWS-foundation stages

These checks are fast enough to run on every PR and on protected branch pushes without turning CI into a slow feedback loop.

## What Stage 20A intentionally does not run yet

To keep this stage cost-safe and credential-safe, CI does not currently:

- use AWS credentials
- deploy infrastructure or application components
- run `terraform apply`
- start the full Kafka, PostgreSQL, Airflow, Spark, or MinIO stack
- execute the full producer, consumer, warehouse-loader, dbt, or anomaly-detection pipeline
- require repository secrets
- depend on large datasets
- perform expensive Docker build-and-run validation

This is deliberate. The goal of 20A is a dependable fast gate, not full environment simulation.

## What runs in Stage 20B

Stage 20B adds a dedicated workflow at `.github/workflows/terraform-validate.yml`.

This workflow runs on pull requests targeting `develop` and `main`, plus direct pushes to those branches, and uses path filters so it focuses on Terraform and workflow-related changes.

The workflow performs these checks inside `infra/aws-orchestration/`:

- repository checkout
- Terraform CLI setup
- `terraform fmt -check`
- `terraform init -backend=false`
- `terraform validate`

## Why Terraform validation is now separate

Terraform validation is split out into its own GitHub Actions quality gate so infrastructure checks stay:

- easy to reason about
- fast to troubleshoot
- independent from the lighter Stage 20A repository checks
- ready to grow later without bloating the base CI workflow

This separation also makes it easier to evolve infrastructure validation cadence and path filters without changing the general-purpose CI foundation.

## Why `terraform init -backend=false` is used

The Stage 20B workflow uses `terraform init -backend=false` to initialize providers and module metadata without connecting to any remote backend.

That matters because it keeps the workflow safe for pull requests:

- no remote state access is required
- no backend credentials are needed
- no cloud resources are created
- no deployment action is performed

Combined with `terraform fmt -check` and `terraform validate`, this gives useful configuration feedback while staying cost-safe and credentials-free.

## Planned Stage 20 expansion

The Stage 20 roadmap is intentionally incremental:

- Stage 20C: add targeted language-level test and lint quality gates
- Stage 20D: add deeper dbt, Airflow, and orchestration validation in modular workflows
- Stage 20E: add stricter container smoke checks and release-oriented safeguards for deployment readiness and branch protection support

Each follow-up stage should stay explicit about runtime cost, credential requirements, and what is safe to execute on GitHub-hosted runners.
