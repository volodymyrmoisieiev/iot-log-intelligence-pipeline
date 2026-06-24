# Stage 20 CI Quality Gates

## What Stage 20 adds

Stage 20 starts the repository's GitHub Actions quality-gate path. Stage 20A introduces a safe, fast CI foundation that runs on pull requests targeting `develop` and `main`, plus direct pushes to those branches. Stage 20B adds a separate Terraform validation workflow for the AWS orchestration foundation under `infra/aws-orchestration/`. Stage 20C adds a dedicated Python and Airflow validation workflow for lightweight syntax and DAG checks. Stage 20D adds pull request and release-process guidance plus visible CI quality indicators in the README.

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

## What runs in Stage 20C

Stage 20C adds a dedicated workflow at `.github/workflows/python-airflow-validate.yml`.

This workflow runs on pull requests targeting `develop` and `main`, plus direct pushes to those branches, and uses path filters aimed at Python-relevant areas such as:

- `scripts/**`
- `airflow/**`
- `aws/lambda/**`
- `observability/**`
- the workflow file itself

The workflow performs these checks:

- repository checkout
- Python 3.11 setup
- `py_compile` validation for important Python entry points when they exist:
  - `scripts/run_anomaly_detection.py`
  - `scripts/validate_data_contract.py`
  - `scripts/run_performance_benchmark.py`
  - `scripts/analyze_performance_results.py`
  - `scripts/create_dataset_profile.py`
  - `aws/lambda/iot_metadata_validator/handler.py`
  - `observability/write_pipeline_observability.py`
- `py_compile` validation for Python DAG files under `airflow/dags/`
- a lightweight listing of detected DAG Python files for easier debugging

If a planned or future file is not present, the workflow skips it rather than failing only because the repository layout evolved.

## Why Python and Airflow validation is separate

Python utility scripts and Airflow DAG files change for different reasons than Terraform or repository-structure updates, so Stage 20C keeps them in their own quality gate.

That separation keeps the workflow:

- lightweight
- easy to troubleshoot
- focused on syntax and DAG safety
- independent from infrastructure validation

## What Stage 20C intentionally does not execute

To keep this workflow safe and fast, Stage 20C does not:

- start Kafka, PostgreSQL, Spark, or MinIO
- run the full local Airflow pipeline
- trigger DAG runs
- require AWS credentials or repository secrets
- execute the full dataset
- perform full Airflow runtime integration checks inside GitHub Actions

For now, Airflow validation in CI is intentionally limited to DAG Python syntax checks and DAG-file discovery. Full Airflow CLI or runtime validation remains better suited to local manual verification through the existing Docker Compose Airflow setup.

## What Stage 20D adds

Stage 20D adds process and presentation improvements around the existing quality gates:

- a GitHub pull request template at `.github/pull_request_template.md`
- a release and branch-cleanup checklist at `docs/release-checklist.md`
- README CI badges for:
  - `CI`
  - `Terraform Validate`
  - `Python and Airflow Validate`

## Why Stage 20D improves the engineering workflow

Stage 20D does not add new runtime validation logic. Instead, it makes the current CI system easier to use and easier to understand:

- contributors get a structured PR checklist before merge
- release flow expectations are documented in one place
- reviewers can see workflow health directly from the README
- the repository looks more polished and portfolio-ready

This is especially useful for a project that now has multiple separate quality gates rather than one large catch-all workflow.

## What Stage 20D intentionally does not change

Stage 20D does not:

- change Docker Compose runtime logic
- change Airflow DAG logic
- change producer, consumer, dbt, Spark, MinIO, or Terraform resource behavior
- add deployment automation
- change GitHub Actions execution logic beyond presentation and process documentation

## What comes next in Stage 20E

Stage 20E is planned to build on the process foundation from 20D with deeper release-readiness and validation coverage, such as:

- broader language-level test automation
- richer dbt or orchestration validation where cost-safe
- stricter smoke-check guidance for higher-confidence merge readiness
- more explicit release guardrails as the repository moves closer to a fuller CI/CD story

## Planned Stage 20 expansion

The Stage 20 roadmap is intentionally incremental:

- Stage 20E: add deeper release-readiness, testing, and higher-confidence validation guardrails on top of the current quality-gate foundation

Each follow-up stage should stay explicit about runtime cost, credential requirements, and what is safe to execute on GitHub-hosted runners.
